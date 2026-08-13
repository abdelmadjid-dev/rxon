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

package com.benaether.rxon.sample;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.util.Log;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import com.benaether.rxon.core.RxOnConfig;
import com.benaether.rxon.rx.RxOnLogger;
import com.benaether.rxon.schedulers.WorkScheduler;

import java.util.concurrent.Executors;

import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;

public final class RxOnInitializer extends ContentProvider {

    private static Scheduler createSingleThreadScheduler(String name) {
        return Schedulers.from(Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, name);
            t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }));
    }

    @Override
    public boolean onCreate() {
        RxOnConfig.builder()
                .debug(true)
                .logger(new RxOnLogger() {
                    @Override
                    public void e(String tag, String message, Throwable t) {
                        Log.e(tag, message, t);
                    }

                    @Override
                    public void w(String tag, String message, Throwable t) {
                        Log.w(tag, message, t);
                    }

                    @Override
                    public void i(String tag, String message) {
                        Log.i(tag, message);
                    }

                    @Override
                    public void d(String tag, String message) {
                        Log.d(tag, message);
                    }
                })
                .errorMapper(ApiErrorMapper::map)
                .scheduler(WorkScheduler.DATA_READ, createSingleThreadScheduler("data-read-1t"))
                .scheduler(WorkScheduler.DATA_WRITE, createSingleThreadScheduler("data-write-1t"))
                .scheduler(WorkScheduler.IO, createSingleThreadScheduler("io-thread-1t"))
                .scheduler(WorkScheduler.COMPUTE, createSingleThreadScheduler("compute-thread-1t"))
                .init();
        return true;
    }

    @Nullable @Override public Cursor query(@NonNull Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) { return null; }
    @Nullable @Override public String getType(@NonNull Uri uri) { return null; }
    @Nullable @Override public Uri insert(@NonNull Uri uri, ContentValues values) { return null; }
    @Override public int delete(@NonNull Uri uri, String selection, String[] selectionArgs) { return 0; }
    @Override public int update(@NonNull Uri uri, ContentValues values, String selection, String[] selectionArgs) { return 0; }
}

