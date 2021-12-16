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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.spi.DefaultBindingTargetVisitor;
import com.google.inject.spi.DefaultElementVisitor;
import com.google.inject.spi.Element;
import com.google.inject.spi.Elements;
import com.google.inject.spi.LinkedKeyBinding;
import com.google.inject.spi.ProviderInstanceBinding;
import com.google.inject.spi.ProviderWithDependencies;
import io.netty.util.SuppressForbidden;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Development-time tools for working with Guice.
 */
public class Tools
{
  /**
   * Dump to stdout a listing of the keys in the given injector, and its
   * parent injector, if any.
   */
  public static String guiceMap(Injector injector)
  {
    StringBuilder buf = new StringBuilder();
    if (injector.getParent() != null) {
      buf.append("-- Parent --\n");
      buf.append(guiceMap(injector.getParent()));
      buf.append("----\n");
    }
    for (Entry<Key<?>, Binding<?>> entry : injector.getBindings().entrySet()) {
      buf.append(StringUtils.format("%s: %s\n",
          entry.getKey().toString(),
          entryToString(entry.getValue())));
    }
    return buf.toString();
  }

  public static String entryToString(Binding<?> binding)
  {
    StringBuilder buf = new StringBuilder();
    buf.append(binding.getClass().getSimpleName());
    buf.append("(");
    Provider<?> provider = binding.getProvider();
    if (provider == null) {
      buf.append("null");
    } else {
      buf.append(provider.getClass().getSimpleName());
    }
    buf.append(")");
    return buf.toString();
  }

  @VisibleForTesting
  @SuppressForbidden(reason = "System#out")
  public static void printMap(Injector injector)
  {
    System.out.println("Guice map");
    System.out.print(guiceMap(injector));
  }

  public static class Replacement
  {
    final Key<?> key;
    final Module originalModule;
    final Module overrideModule;

    public Replacement(
        Key<?> key,
        Module originalModule,
        Module overrideModule
    )
    {
      this.key = key;
      this.originalModule = originalModule;
      this.overrideModule = overrideModule;
    }

    @Override
    public String toString()
    {
      return "[" + key.toString() +
             ",\n  original=" + originalModule.getClass().getSimpleName() +
             ",\n  override=" + overrideModule.getClass().getSimpleName() +
             "]";
    }
  }

  @SuppressForbidden(reason = "System#out")
  public static class OverrideAnalyzer
  {
    private final Map<Key<?>, Module> bindings = new HashMap<>();
    private final List<Replacement> replacements = new ArrayList<>();

    public void add(Module module)
    {
      for (Element element : Elements.getElements(module)) {
        element.acceptVisitor(new DefaultElementVisitor<Void>()
        {
          @Override
          public <T> Void visit(Binding<T> binding)
          {
            Key<T> key = binding.getKey();
            Module current = bindings.get(key);
            if (current != null) {
              boolean isReplacement = binding.acceptTargetVisitor(
                  new DefaultBindingTargetVisitor<Object, Boolean>()
                  {
                    @Override
                    protected Boolean visitOther(Binding<? extends Object> binding)
                    {
                      return true;
                    }

                    @Override
                    public Boolean visit(ProviderInstanceBinding<? extends Object> providerInstanceBinding)
                    {
                      Provider<?> providerInstance = providerInstanceBinding.getProviderInstance();
                      if (providerInstance instanceof Multibinder
                          || providerInstance instanceof ProviderWithDependencies) {
                        // This is a multibinding, so no actual override
                        return false;
                      }
                      return true;
                    }

                    @Override
                    public Boolean visit(LinkedKeyBinding<? extends Object> linkedKeyBinding)
                    {
                      // Used as part of the multibinding. Ignore this one.
                      return false;
                    }

                  });
              if (isReplacement) {
                Replacement replacement = new Replacement(key, current, module);
                replacements.add(replacement);
                System.out.println(replacement);
              }
            }
            bindings.put(key, module);
            return null;
          }
        });
      }
    }

    public List<Pair<Module, List<Module>>> dependencies()
    {
      Map<Module, Map<Module, Boolean>> overrides = new IdentityHashMap<>();
      for (Replacement replacement : replacements) {
        Map<Module, Boolean> predecessors = overrides.get(replacement.overrideModule);
        if (predecessors == null) {
          predecessors = new IdentityHashMap<Module, Boolean>();
          overrides.put(replacement.overrideModule, predecessors);
        }
        predecessors.put(replacement.originalModule, true);
      }
      List<Pair<Module, List<Module>>> deps = new ArrayList<>();
      for (Entry<Module, Map<Module, Boolean>> entry : overrides.entrySet()) {
        List<Module> preds = new ArrayList<>(entry.getValue().keySet());
        deps.add(new Pair<>(entry.getKey(), preds));
      }
      return deps;
    }

    @SuppressForbidden(reason = "System#out")
    public void printDependencies()
    {
      if (replacements.isEmpty()) {
        return;
      }
      System.out.println("Dependencies:");
      for (Pair<Module, List<Module>> item : dependencies()) {
        System.out.println(item.lhs.getClass().getSimpleName());
        for (Module dep : item.rhs) {
          System.out.print("  ");
          System.out.println(dep.getClass().getSimpleName());
        }
      }
    }
  }
}
