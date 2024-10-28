package org.example.struct;

public class Key {
    private String temporalKey;
    private String spatioKey;

    public Key(String tKey, String sKey){
        this.temporalKey = tKey;
        this.spatioKey = sKey;
    }

    public String getKey(){
        StringBuffer sb = new StringBuffer();
        for(int i = 0; i < temporalKey.length(); i++){
            sb.append(temporalKey.charAt(i));
            sb.append(spatioKey.charAt(i));
        }
        return sb.toString();
    }
}
