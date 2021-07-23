package org.apache.calcite.plan.dqn;

/**
 * 依据训练的代价模型
 */
public enum CostModel {

    COST_MODEL_1;

    public String toDataFileName(){
        return this.name() + ".data";
    }

    public String toModelFileName(){
        return this.name() + ".model";
    }
}
