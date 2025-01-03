/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.daemon.scaling.policy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PidPolicy extends Policy
{
    private static final Logger log = LogManager.getLogger(BasicPolicy.class);
    // add a control to make scaling-choice
    class pidcontrollor
    {
        private float kp, ki, kd;
        private int queryConcurrencyTarget, integral, prerror;
        pidcontrollor(){
            this.kp = 0.8F;
            this.ki = 0.05F;
            this.kd = 0.05F;
            this.queryConcurrencyTarget = 1;
            this.integral = 0;
            this.prerror = 0;
        }
        public float calcuator(int queryConcurrency){
            int error = queryConcurrency - queryConcurrencyTarget;
            integral += error;
            int derivative = error - prerror;
            float re = kp*error + ki*integral + kd*derivative;
            prerror = error;
            return re;
        }
        public void argsuit(){ // finetuing the  arg:kp ki kd
            return;
        }
    }

    private pidcontrollor ctl = new pidcontrollor();
    @Override
    public void doAutoScaling()
    {
        int queryConcurrency = metricsQueue.getLast();
        System.out.println("Receive metrics:" + metricsQueue);
        float sf = ctl.calcuator(queryConcurrency);
        log.info("INFO: expand " + sf +" vm");
        scalingManager.multiplyInstance(sf);
    }

}
