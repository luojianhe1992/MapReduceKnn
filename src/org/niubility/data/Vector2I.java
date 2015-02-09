package org.niubility.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Vector2I extends Vector2<Integer, Integer> implements Writable{
        public Vector2I() {
        }

        public Vector2I(Integer v1, Integer v2) {
                this.v1 = v1;
                this.v2 = v2;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
        	v1 = in.readInt();
        	v2 = in.readInt();
        }

        @Override
        public void write(DataOutput out) throws IOException {
                out.writeInt(v1);
                out.writeInt(v2);
        }
}