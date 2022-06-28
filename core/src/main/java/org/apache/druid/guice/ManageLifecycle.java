/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.guice;

import com.google.inject.ScopeAnnotation;
import org.apache.druid.guice.annotations.PublicApi;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Guice binding scope for lifecycle-managed instances.
 * Marks the object to be managed by
 * {@link org.apache.druid.java.util.common.lifecycle.Lifecycle} and set to be on
 * {@link org.apache.druid.java.util.common.lifecycle.Lifecycle.Stage#NORMAL}
 * stage. This stage gets defined by {@link LifecycleModule}.
 * <p>
 * Best practice is to explicitly bind the instance via Guice. Example:
 * <code><pre>
 * binder.bind(MyClass.class).in(ManageLifecycle.class);</pre></code>
 * <p>
 * Guice allows implicit scope by putting the scope annotation on the class
 * itself. You'll see instances of such a pattern in the code base. However,
 * explicit binding is a better practice.
 */
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@ScopeAnnotation
@PublicApi
public @interface ManageLifecycle
{
}
