package org.example.struct;

public class Key implements Comparable<Key>{
    private String temporalKey;
    private String spatioKey;

    public Key(String tKey, String sKey){
        this.temporalKey = tKey;
        this.spatioKey = sKey;
    }

    public String getKey(){
        StringBuffer sb = new StringBuffer();
        for(int i = 0; i < spatioKey.length(); i++){
            // sb.append(temporalKey.charAt(i));
            sb.append(spatioKey.charAt(i));
        }
        return sb.toString();
    }

    @Override
    public String toString() {
      return this.getKey();
    }

    @Override
    public int compareTo(Key other) {
        return this.getKey().compareTo(other.getKey());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Key)) {
            return false;
        }
        Key other = (Key) obj;
        return this.getKey().equals(other.getKey());
    }

    @Override
    public int hashCode() {
        return this.getKey().hashCode();
    }
}
