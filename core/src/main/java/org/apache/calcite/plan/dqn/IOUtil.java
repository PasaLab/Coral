package org.apache.calcite.plan.dqn;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.jdbc.util.GlobalInfo;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.indexing.NDArrayIndex;

import java.io.*;

/**
 * <ol>
 *     <li>Read / Write Train Data File</li>
 *     <li>Read / Write DQN Model File (DL4j has done)</li>
 *     <li>RelNode 2 INDArray</li>
 *     <li>Convention 2 INDArray</li>
 * </ol>
 */
@Slf4j
public class IOUtil {

    public static void writeTrainingData(INDArray input, double cost, CostModel costModel) {
        double[] labelArray = new double[1];
        labelArray[0] = cost;
        INDArray label = Nd4j.create(labelArray, 1, 1);
        INDArray row = Nd4j.concat(1, input, label);
        try (DataOutputStream sWrite = new DataOutputStream(new FileOutputStream(new File(costModel.toDataFileName()), true))) {
            Nd4j.write(row, sWrite);
        } catch (IOException e) {
            log.warn("writing training data failed: {}", costModel.toDataFileName());
        }
    }

    public static DataSet readTrainingData(CostModel costModel) {
        INDArray currRead;
        INDArray arrRead = null;
        try (DataInputStream sRead = new DataInputStream(new FileInputStream(new File(costModel.toDataFileName())))) {
            while (sRead.available() > 0 && (currRead = Nd4j.read(sRead)) != null) {
                if (arrRead == null) {
                    arrRead = currRead;
                } else {
                    arrRead = Nd4j.concat(0, arrRead, currRead);
                }
            }
        } catch (IOException e) {
            log.warn("reading training data failed: {}", costModel.toDataFileName());
            return DataSet.empty();
        }
        int columnSize = GlobalInfo.getInstance().getFeatureSize() + 1;
        return new DataSet(arrRead.get(NDArrayIndex.all(), NDArrayIndex.interval(0, columnSize - 1)),
                arrRead.get(NDArrayIndex.all(), NDArrayIndex.interval(columnSize - 1, columnSize)));
//        String line;
//        List<DataSet> totalDataSetList = new LinkedList<>();
//        try (FileReader fr = new FileReader(costModel.toDataFileName());
//             BufferedReader br = new BufferedReader(fr)) {
//            while ((line = br.readLine()) != null) {
//                String[] token = line.trim().split(",");
//                double[] featureArray = new double[token.length - 1];
//                double[] labelArray = new double[1];
//                for (int i = 0; i < token.length - 1; ++i) {
//                    featureArray[i] = Double.parseDouble(token[i]);
//                }
//                labelArray[0] = Double.parseDouble(token[token.length - 1]);
//                //
//                INDArray featureNDArray = Nd4j.create(featureArray, 1, token.length - 1);
//                INDArray labelNDArray = Nd4j.create(labelArray, 1, 1);
//                totalDataSetList.add(new DataSet(featureNDArray, labelNDArray));
//            }
//        } catch (IOException e) {
//            log.warn("writing training data failed: {}", costModel.toDataFileName());
//        }
//        return totalDataSetList;
    }

    public static INDArray relNode2Vector(RelNode root) {            // make sure global row type is registered
        RelDataType rowType = root.getRowType();
        GlobalInfo globalInfo = GlobalInfo.getInstance();
        int size = globalInfo.getAttrSize();
        // from rowType get attr 0/1 representation
        double[] graphVector = new double[size];
        for (String fieldName : rowType.getFieldNames()) {
            // todo [coral] support selectivity
//            root.getCluster().getMetadataQuery().getSelectivity(root,root.get);
            graphVector[globalInfo.getAttrIndex(fieldName)] = 1;
        }
        return Nd4j.create(graphVector, 1, size);
    }

    public static INDArray convention2Vector(Convention convention) {            // make sure global row type is registered
        GlobalInfo globalInfo = GlobalInfo.getInstance();
        int size = globalInfo.getConventionSize();
        double[] vector = new double[size];
        vector[globalInfo.getConventionIndex(convention.getName())] = 1;
        return Nd4j.create(vector, 1, size);
    }


}
