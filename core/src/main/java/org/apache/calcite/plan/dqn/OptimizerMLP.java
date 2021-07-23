package org.apache.calcite.plan.dqn;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.jdbc.util.GlobalInfo;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.tools.RelBuilder;
import org.deeplearning4j.datasets.iterator.impl.ListDataSetIterator;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.nd4j.evaluation.regression.RegressionEvaluation;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.SplitTestAndTrain;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.lossfunctions.LossFunctions;

import java.io.File;
import java.io.IOException;
import java.util.List;

@Slf4j
public class OptimizerMLP {

    private static MultiLayerNetwork mlp;

    private static boolean isModelReady = false;

    private static boolean isDataReady = false;

    /**
     * if model is ready, dqn is available;
     * // todo [coral] [P10] should judge each n times query (with DQN_SAMPLE_COLLECT true)
     */
    public static boolean isAvailable() {
        return CalciteSystemProperty.DQN_MODEL_READY.value() || isModelReady || isDataReady;
    }

    public static MultiLayerNetwork model(int inputNum, int outputNum) {
        int hiddenNum = GlobalInfo.getInstance().getConfigThreadLocal().get().dqnHiddenNum();
        MultiLayerConfiguration.Builder builder = new NeuralNetConfiguration.Builder()
                .seed(12345L)
                .updater(new Adam())
                .weightInit(WeightInit.XAVIER)
                .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
                .list()
                .layer(0, new DenseLayer.Builder()
                        .activation(Activation.LEAKYRELU)
                        .nIn(inputNum)
                        .nOut(hiddenNum)
                        .build())
                .layer(1, new OutputLayer.Builder(LossFunctions.LossFunction.MEAN_SQUARED_LOGARITHMIC_ERROR)
                        .activation(Activation.IDENTITY)
                        .nIn(hiddenNum)
                        .nOut(outputNum)
                        .build());
        MultiLayerConfiguration conf = builder.build();
        MultiLayerNetwork model = new MultiLayerNetwork(conf);
        model.init();
        return model;
    }

    public static MultiLayerNetwork train(CostModel costModel) throws IOException {
        if (!isAvailable()) {
            log.error("net work can't be trained");
            return null;
        }
        final String targetModelPath = costModel.toModelFileName();
        // load model
        if (new File(targetModelPath).exists()) {
            mlp = MultiLayerNetwork.load(new File(targetModelPath), true);
        } else {
            final long SEED = 1234L;
//            final int trainSize = 400;
            int batchSize = GlobalInfo.getInstance().getConfigThreadLocal().get().dqnBatchSize();
            int epochSize = GlobalInfo.getInstance().getConfigThreadLocal().get().dqnEpochSize();
            //声明多层感知机
            mlp = model(GlobalInfo.getInstance().getFeatureSize(), 1);
            mlp.setListeners(new ScoreIterationListener(100));
            // load && split && normalize data
            DataSet allData = IOUtil.readTrainingData(costModel);
            //获取全部数据并且打乱顺序
//            DataSet allData = DataSet.merge(trainingData);
            allData.shuffle(SEED);
            //划分训练集和验证集
            SplitTestAndTrain split = allData.splitTestAndTrain(0.8);
            DataSet dsTrain = split.getTrain();
            DataSet dsTest = split.getTest();
            DataSetIterator trainIter = new ListDataSetIterator(dsTrain.asList(), batchSize);
            DataSetIterator testIter = new ListDataSetIterator(dsTest.asList(), batchSize);
            //归一化处理
//            DataNormalization scaler = new NormalizerMinMaxScaler(0, 1);
//            scaler.fit(trainIter);
//            scaler.fit(testIter);
//            trainIter.setPreProcessor(scaler);
//            testIter.setPreProcessor(scaler);

            //训练n个epoch
            for (int i = 0; i < epochSize; ++i) {
                mlp.fit(trainIter);
                trainIter.reset();
            }
            RegressionEvaluation eval = mlp.evaluateRegression(testIter);
            log.info("model = {}, test statistics = {}", costModel, eval.stats());
            mlp.save(new File(targetModelPath), true);
            isModelReady = true;
        }
        return mlp;
    }

    public static RelNode execute(RelNode relNode, RelBuilder relBuilder, CostModel costModel) {
        log.debug("Plan before DQN optimize:\n {}",
                RelOptUtil.toString(relNode, SqlExplainLevel.ALL_ATTRIBUTES));
        try {
            mlp = mlp == null ? train(costModel) : mlp;
        } catch (IOException e) {
            log.warn("something went wrong when read model or data file");
            return relNode;
        }
        EpisodeSample episodeSample = EpisodeSample.rel2MDP(relNode, false, CostModel.COST_MODEL_1);
        while (!episodeSample.isDone()) {
            List<EpisodeSample.Action> actionSpace = episodeSample.getActionSpace();
            double minCost = Double.MAX_VALUE;
            int actionIndex = -1;
            for (int i = 0, actionSpaceSize = actionSpace.size(); i < actionSpaceSize; i++) {
                EpisodeSample.Action action = actionSpace.get(i);
                INDArray input = Nd4j.concat(1, episodeSample.getGraph().toVector(), action.toVector());
                INDArray output = mlp.output(input);
                if (output.getDouble(0) < minCost) {
                    minCost = output.getDouble(0);
                    actionIndex = i;
                }
            }
            // choose action
            episodeSample.takeAction(actionSpace.get(actionIndex));
        }
        return episodeSample.mdp2RelNode();
    }


}
