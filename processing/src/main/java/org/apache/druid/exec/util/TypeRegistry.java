package org.apache.druid.exec.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import org.apache.druid.segment.column.ColumnType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Holds type information that should be in {@code ColumnType}, but isn't.
 */
public class TypeRegistry
{
  public interface TypeAttributes
  {
    ColumnType type();
    boolean comperable();
    Ordering<Object> objectOrdering();
  }

  private static class TypeAttribsImpl implements TypeAttributes
  {
    private final ColumnType type;
    private final Ordering<Object> objectOrdering;

    @SuppressWarnings("unchecked")
    public TypeAttribsImpl(ColumnType type, @SuppressWarnings("rawtypes") Ordering objectOrdering)
    {
      this.type = type;
      this.objectOrdering = objectOrdering;
    }

    @Override
    public ColumnType type()
    {
      return type;
    }

    @Override
    public boolean comperable()
    {
      return objectOrdering != null;
    }

    @Override
    public Ordering<Object> objectOrdering()
    {
      return objectOrdering;
    }
  }

  public static final TypeRegistry INSTANCE = new TypeRegistry();

  private final Map<ColumnType, TypeAttributes> types;

  public TypeRegistry()
  {
    List<TypeAttribsImpl> types = new ArrayList<>();
    types.add(new TypeAttribsImpl(ColumnType.STRING, (Ordering<?>) Ordering.natural()));
    types.add(new TypeAttribsImpl(ColumnType.LONG, Ordering.natural()));
    types.add(new TypeAttribsImpl(ColumnType.FLOAT, Ordering.natural()));
    types.add(new TypeAttribsImpl(ColumnType.DOUBLE, Ordering.natural()));
    types.add(new TypeAttribsImpl(ColumnType.UNKNOWN_COMPLEX, null));

    ImmutableMap.Builder<ColumnType, TypeAttributes> builder = ImmutableMap.builder();
    for (TypeAttribsImpl type : types) {
        builder.put(type.type(), type);
    }
    this.types = builder.build();
  }

  public TypeAttributes resolve(ColumnType type) {
    return types.get(type);
  }
}
