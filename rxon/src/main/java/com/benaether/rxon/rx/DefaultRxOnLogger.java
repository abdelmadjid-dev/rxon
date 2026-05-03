/*
 * Copyright 2026 Abdelmadjid
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.benaether.rxon.rx;

public class DefaultRxOnLogger implements RxOnLogger {
    @Override
    public void e(String tag, String msg, Throwable t) {
        System.err.println("[" + tag + "] ERROR: " + msg);
        if (t != null) t.printStackTrace();
    }

    @Override
    public void w(String tag, String msg, Throwable t) {
        System.out.println("[" + tag + "] WARN: " + msg);
        if (t != null) t.printStackTrace();
    }

    @Override
    public void i(String tag, String msg) {
        System.out.println("[" + tag + "] INFO: " + msg);
    }

    @Override
    public void d(String tag, String msg) {
        System.out.println("[" + tag + "] DEBUG: " + msg);
    }
}

