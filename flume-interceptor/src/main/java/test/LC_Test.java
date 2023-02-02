package test;

/**
 * @Author Joisen
 * @Date 2023/2/1 10:51
 * @Version 1.0
 */
public class LC_Test {

    public static String decodeMessage(String key, String message) {
        char[] map = new char[26];
        for (int i = 0; i < map.length; i++) {
            System.out.println(map[i]);
        }
        char curChar = 'a';
        for(char i : key.toCharArray()){
            if(i != ' ' && map[i - 'a'] == 0){
                map[i - 'a'] = curChar ++;
                System.out.println("map[i-'a'] = " + map[i - 'a'] + " i = " + i);
            }
        }
        char[] res = message.toCharArray();
        for(int i=0; i< res.length; i++){
            if(res[i] != ' '){
                res[i] = map[res[i] - 'a'];
            }
        }
        return new String(res);
    }

    public static int divide(int a, int b) {
        double res = 0;
        int flag = 1; // 1 为正
        if( (a < 0 && b > 0) || (a > 0 && b < 0)) flag = 0;
        a = Math.abs(a);
        b = Math.abs(b);
        while(a >= b){
            res ++;
            a -= b;
        }
        if(flag == 0) res = -res;

        return truncate(res);
    }
    public static int truncate(double tmp){
        if(tmp > 0){
            return (int)Math.floor(tmp);
        }else{
            return (int)Math.ceil(tmp);
        }
    }

    public static void main(String[] args) {
//        String key = "the quick brown fox jumps over the lazy dog", message = "vkbs bs t suepuv";
//        String s = decodeMessage(key, message);

//        int divide = divide(-2147483648,-1);
//        System.out.println(divide);
//        System.out.println( Math.pow(2,31) );
        int a = 1, b = -1;
        System.out.println((a > 0) ^ (b > 0) ? -1 : 1);
    }

}
