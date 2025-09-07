package bqstorageapi.handler;

@FunctionalInterface
public interface ValueMapper<T> {
    T map(Object value); // value có thể là scalar hoặc phần tử của mảng
}
