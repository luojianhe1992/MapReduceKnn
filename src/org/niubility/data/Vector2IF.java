package org.niubility.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Vector2IF extends Vector2<Integer, Float> implements Writable {
        public Vector2IF() {
        }

        public Vector2IF(Integer v1, Float v2) {
                this.v1 = v1;
                this.v2 = v2;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            //v1 means the label    
        	v1 = in.readInt();
            //v2 means the distance
        	v2 = in.readFloat();
        }

        @Override
        public void write(DataOutput out) throws IOException {
                out.writeInt(v1);
                out.writeFloat(v2);
        }
}
