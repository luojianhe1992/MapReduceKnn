/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.niubility.data;

/**
 *
 * @author LIUSONG
 */
public class EuclideanDistance implements DistanceMeasure {

    @Override
    public double distance(SparseVector lh, SparseVector rh) {
        double res = 0;

        for (String k : lh.keySet()) {
            if (rh.containsKey(k)) {
                res = Math.sqrt(Math.pow(res, 2)
                        + Math.pow(lh.get(k) - rh.get(k), 2));
            } else {
                //   res = Math.sqrt(lh.get(k) * lh.get(k) + Math.pow(res, 2));
            }
        }
        return res;
    }
}
