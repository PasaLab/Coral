package org.apache.calcite.plan.dqn;

import lombok.Getter;
import org.deeplearning4j.gym.StepReply;
import org.deeplearning4j.rl4j.mdp.MDP;
import org.deeplearning4j.rl4j.space.ActionSpace;
import org.deeplearning4j.rl4j.space.ArrayObservationSpace;
import org.deeplearning4j.rl4j.space.Encodable;
import org.deeplearning4j.rl4j.space.ObservationSpace;

import java.util.List;

/**
 * not used now
 */
@Deprecated
public class OptimizerMDP implements MDP<OptimizerMDP.Graph, OptimizerMDP.Join, OptimizerMDP.JoinSpace> {

    // 收集的训练数据
    private List<Record> dataset;
    private int offset;

    private double reward;
    private Graph currState;

    private JoinSpace actionSpace;
    private ObservationSpace<Graph> observationSpace;

    public OptimizerMDP(int attrNum, int platformNum) {
        observationSpace = new ArrayObservationSpace<>(new int[]{attrNum});
        actionSpace = new JoinSpace(this, platformNum + attrNum * 2);
    }

    public OptimizerMDP() {
        // demo 三张表，九个属性
        this(9, 3);
    }

    @Override
    public ObservationSpace<Graph> getObservationSpace() {
        return observationSpace;
    }

    @Override
    public JoinSpace getActionSpace() {
        return actionSpace;
    }

    @Override
    public Graph reset() {
        reward = 0;
        offset++;
        currState = getCurrRecord().state;
        return currState;
    }

    @Override
    public void close() {
    }

    @Override
    public StepReply<Graph> step(Join action) {
        reward += getCurrRecord().cost;
        offset++;
        currState = getCurrRecord().state;
        return new StepReply<>(currState, reward, isDone(), null);
    }

    @Override
    public boolean isDone() {
        return currState.connectedComponent == 1;
    }

    @Override
    public MDP<Graph, Join, JoinSpace> newInstance() {
        return null;
    }

    private Record getCurrRecord() {
        if (offset >= dataset.size()) {
            throw new StackOverflowError();
        }
        return dataset.get(offset);
    }


    @Getter
    static class Graph implements Encodable {
        double[] vector;
        int connectedComponent;

        @Override
        public double[] toArray() {
            return vector;
        }
    }

    static class Join {
        double[] left;
        double[] right;
        double[] platform;
    }

    static class JoinSpace implements ActionSpace<Join> {

        OptimizerMDP mdp;
        int vectorLength;

        public JoinSpace(OptimizerMDP mdp, int vectorLength) {
            this.mdp = mdp;
            this.vectorLength = vectorLength;
        }

        @Override
        public Join randomAction() {
            return mdp.getCurrRecord().action;
        }

        @Override
        public Object encode(Join action) {
            return action;
        }

        @Override
        public int getSize() {
            return vectorLength;
        }

        @Override
        public Join noOp() {
            return mdp.getCurrRecord().action;
        }
    }

    public static class Record {
        Graph state;
        Join action;
        Graph newState;
        Double cost;
    }
}
