package brickhouse.testdriver;


public class test {
    public static void main(String[] args) {

        int [] a={1,3,7,8,9,11,15};
        for(int flag:a){
            System.out.print(flag&1);
            System.out.print(flag&2);
            System.out.print(flag&4);
            System.out.print(flag&8);
            System.out.println("");
        }




    }
}
