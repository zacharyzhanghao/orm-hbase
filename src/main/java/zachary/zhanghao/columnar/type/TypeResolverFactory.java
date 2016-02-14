/**
 * 
 */
package zachary.zhanghao.columnar.type;

import java.util.Date;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * @author zachary.zhang
 *
 */
public class TypeResolverFactory {


    private static Map<Class<?>, TypeResolver<?>> defaultResolvers = Maps.newHashMap();

    static {
        defaultResolvers.put(String.class, new StringResolver());

        defaultResolvers.put(Date.class, new DateResolver());

        defaultResolvers.put(Boolean.class, new BooleanResolver());
        defaultResolvers.put(boolean.class, new BooleanResolver());

        defaultResolvers.put(Byte.class, new ByteResolver());
        defaultResolvers.put(byte.class, new ByteResolver());

        defaultResolvers.put(Short.class, new ShortResolver());
        defaultResolvers.put(short.class, new ShortResolver());

        defaultResolvers.put(Integer.class, new IntegerResolver());
        defaultResolvers.put(int.class, new IntegerResolver());

        defaultResolvers.put(Long.class, new LongResolver());
        defaultResolvers.put(long.class, new LongResolver());

        defaultResolvers.put(Float.class, new FloatResolver());
        defaultResolvers.put(float.class, new FloatResolver());

        defaultResolvers.put(Double.class, new DoubleResolver());
        defaultResolvers.put(double.class, new DoubleResolver());
    }

    public static TypeResolver<?> getResolver(Class<?> type) {
        Preconditions.checkNotNull(type, "type can't be null");

        return defaultResolvers.get(type);
    }

    public static class BooleanResolver extends TypeResolverBase<Boolean> {

        @Override
        public byte[] toBytes(Boolean object) {
            return Bytes.toBytes(object);
        }

        @Override
        public Boolean toObject(byte[] bytes) {
            return Bytes.toBoolean(bytes);
        }
    }

    public static class ByteResolver extends TypeResolverBase<Byte> {

        @Override
        public byte[] toBytes(Byte object) {
            Preconditions.checkArgument(accept(object), "target type can't be match Byte");
            return new byte[] {((Byte) object).byteValue()};
        }

        @Override
        public Byte toObject(byte[] bytes) {
            Preconditions.checkState(bytes != null && bytes.length > 1,
                            "byte array is null or lenght <1");
            return bytes[0];
        }
    }

    public static class DateResolver extends TypeResolverBase<Date> {

        @Override
        public byte[] toBytes(Date object) {
            Preconditions.checkArgument(accept(object), "target type can't be match Date");
            return Bytes.toBytes(object.getTime());
        }

        @Override
        public Date toObject(byte[] bytes) {
            long date = Bytes.toLong(bytes);
            return new Date(date);
        }
    }

    public static class DoubleResolver extends TypeResolverBase<Double> {

        @Override
        public byte[] toBytes(Double object) {
            Preconditions.checkArgument(accept(object), "target type can't be match Double");
            return Bytes.toBytes(object);
        }

        @Override
        public Double toObject(byte[] bytes) {
            return Bytes.toDouble(bytes);
        }

    }

    public static class FloatResolver extends TypeResolverBase<Float> {

        @Override
        public byte[] toBytes(Float object) {
            Preconditions.checkArgument(accept(object), "target type can't be match Float");

            return Bytes.toBytes(object);
        }

        @Override
        public Float toObject(byte[] bytes) {
            return Bytes.toFloat(bytes);
        }
    }

    public static class IntegerResolver extends TypeResolverBase<Integer> {

        @Override
        public byte[] toBytes(Integer object) {
            Preconditions.checkArgument(accept(object), "target type can't be match Integer");

            return Bytes.toBytes(object);
        }

        @Override
        public Integer toObject(byte[] bytes) {
            return Bytes.toInt(bytes);
        }
    }

    public static class LongResolver extends TypeResolverBase<Long> {

        @Override
        public byte[] toBytes(Long object) {
            Preconditions.checkArgument(accept(object), "target type can't be match Long");

            return Bytes.toBytes(object);
        }

        @Override
        public Long toObject(byte[] bytes) {
            return Bytes.toLong(bytes);
        }
    }

    public static class ShortResolver extends TypeResolverBase<Short> {

        @Override
        public byte[] toBytes(Short object) {
            Preconditions.checkArgument(accept(object), "target type can't be match Short");

            return Bytes.toBytes(object);
        }

        @Override
        public Short toObject(byte[] bytes) {
            return Bytes.toShort(bytes);
        }
    }

    public static class StringResolver extends TypeResolverBase<String> {

        @Override
        public byte[] toBytes(String object) {
            Preconditions.checkArgument(accept(object), "target type can't be match String");

            return Bytes.toBytes(object);
        }

        @Override
        public String toObject(byte[] bytes) {
            return Bytes.toString(bytes);
        }
    }
}
