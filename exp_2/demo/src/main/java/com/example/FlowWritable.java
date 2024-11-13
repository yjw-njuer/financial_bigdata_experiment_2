package com.example;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowWritable implements Writable {
    private DoubleWritable purchaseAmount;
    private DoubleWritable redeemAmount;

    public FlowWritable() {
        this.purchaseAmount = new DoubleWritable(0);
        this.redeemAmount = new DoubleWritable(0);
    }

    public FlowWritable(double purchaseAmount, double redeemAmount) {
        this.purchaseAmount = new DoubleWritable(purchaseAmount);
        this.redeemAmount = new DoubleWritable(redeemAmount);
    }

    public DoubleWritable getPurchaseAmount() {
        return purchaseAmount;
    }

    public DoubleWritable getRedeemAmount() {
        return redeemAmount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        purchaseAmount.write(out);
        redeemAmount.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        purchaseAmount.readFields(in);
        redeemAmount.readFields(in);
    }

    @Override
    public String toString() {
        return purchaseAmount.get() + "," + redeemAmount.get();
    }
}
