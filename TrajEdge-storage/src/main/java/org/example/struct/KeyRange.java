package org.example.struct;

import java.util.NoSuchElementException;
import java.util.Iterator;

public class KeyRange {
    private static final Key INVALID_KEY = new Key("", "9999999");
    private Key startKey;
    private Key endKey;

    // TODO 假设起始key和终止key要么成包含关系,要么成同级关系
    public KeyRange(Key st, Key en){
        this.startKey = st;
        this.endKey = en;
    }

    public String commonPrefix(){
        String st = startKey.getKey();
        String en = endKey.getKey();
        int i = 0;
        for(; i < st.length(); i++){
            if(st.charAt(i) != en.charAt(i))return st.substring(0, i - 1);
        }
        return st;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof KeyRange)) {
            return false;
        }
        KeyRange other = (KeyRange) obj;
        return this.startKey.equals(other.startKey) && this.endKey.equals(other.endKey);
    }

    @Override
    public int hashCode() {
        return startKey.hashCode() + endKey.hashCode();
    }

    // 新增的迭代器类
    public Iterator<Key> iterator() {
        return new KeyIterator();
    }

    private class KeyIterator implements Iterator<Key> {
        private Key current;

        public KeyIterator() {
            this.current = startKey;
        }

        @Override
        public boolean hasNext() {
            return current.compareTo(endKey) <= 0;
        }

        @Override
        public Key next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Key nextKey = current;
            current = getNextKey(current);
            return nextKey;
        }

        private Key getNextKey(Key key) {
            // 生成下一个键的逻辑
            String keyStr = key.getKey();
            String endKeyStr = endKey.getKey();
            StringBuilder nextKey = new StringBuilder(keyStr);

            if(nextKey.length() < endKeyStr.length()){
                nextKey.append(endKeyStr.charAt(nextKey.length()));
                return new Key("", nextKey.toString());
            }
            // length equal
            int count = 0;
            for (int i = nextKey.length() - 1; i > 0; i--) {
                char c = nextKey.charAt(i);
                if (c < '3') {
                    nextKey.setCharAt(i, (char) (c + count + 1));
                    return new Key("", nextKey.toString());
                } else {
                    nextKey.setCharAt(i, '0'); // 进
                    count++;
                }
            }
            return INVALID_KEY; // 如果全是3，返回下一个长度的键
        }
    }

    @Override
    public String toString() {
      return "[" +this.startKey + ", " + this.endKey + "]";
    }
}
