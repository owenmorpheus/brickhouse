package brickhouse.udf.bitmap;

public class ExistNumberInArray {

    public static void main(String[] args) {

        int sum = sum(1, 2, 23, 23, 232, 32, 23);
        System.out.println(sum);


    }
    public static int sum(int ...strs){
        int a=0;
        for(int x:strs){
            a=x+a;
        }
        return a;
    }
}
