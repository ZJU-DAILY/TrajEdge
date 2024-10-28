package org.example.struct;

public class KeyRange {
    private Key startKey;
    private Key endKey;

    public KeyRange(Key st, Key en){
        this.startKey = st;
        this.endKey = en;
    }

    public String commonPrefix(){
        String st = startKey.getKey();
        String en = endKey.getKey();
        int i = 0;
        for(; i < st.length(); i++){
            if(st.charAt(i) != en.charAt(i))break;
        }
        return st.substring(0, i - 1);
    }

}
