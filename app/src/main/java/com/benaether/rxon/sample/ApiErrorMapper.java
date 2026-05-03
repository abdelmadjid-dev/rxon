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

import com.benaether.rxon.sample.exceptions.BadRequestException;
import com.benaether.rxon.sample.exceptions.ForbiddenException;
import com.benaether.rxon.sample.exceptions.HttpException;
import com.benaether.rxon.sample.exceptions.InternalServerException;
import com.benaether.rxon.sample.exceptions.NetworkException;
import com.benaether.rxon.sample.exceptions.NotFoundException;
import com.benaether.rxon.sample.exceptions.RateLimitExceededException;
import com.benaether.rxon.sample.exceptions.UnauthorizedException;

import java.io.IOException;

public final class ApiErrorMapper {

    private ApiErrorMapper() {}

    public static Throwable map(Throwable throwable) {

        if (throwable instanceof retrofit2.HttpException) {
            int code = ((retrofit2.HttpException) throwable).code();
            return mapHttpCode(code);
        }

        if (throwable instanceof IOException) {
            return new NetworkException("Network error", throwable);
        }

        return throwable;
    }

    private static Throwable mapHttpCode(int statusCode) {
        return switch (statusCode) {
            case 400 -> new BadRequestException("Bad Request");
            case 401 -> new UnauthorizedException("Unauthorized Access");
            case 403 -> new ForbiddenException("Forbidden");
            case 404 -> new NotFoundException("Resource Not Found");
            case 429 -> new RateLimitExceededException("Rate Limit Exceeded");
            case 500 -> new InternalServerException("Internal Server Error");
            default -> new HttpException("HTTP error: " + statusCode);
        };
    }
}

