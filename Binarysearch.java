import java.util.*;
import java.sql.*;
import java.io.*;


public class Binarysearch{

	public boolean compareResultSets(ResultSet resultSet1, ResultSet resultSet2) throws SQLException{
        
		while (resultSet1.next()) {
            
        	resultSet2.next();
            ResultSetMetaData resultSetMetaData = resultSet1.getMetaData();
            int count = resultSetMetaData.getColumnCount();
            for (int i = 1; i <= count; i++) {
                if (!resultSet1.getObject(i).equals(resultSet2.getObject(i))) {
                    return false;
                }
            }
           
        }
        
        if(resultSet2.next())
        	return false;
        
        return true;
    }
	

	
	public static void main(String[] args) {
      String JDriver="com.microsoft.sqlserver.jdbc.SQLServerDriver";
//SQL数据库引擎
      //String connectDB="jdbc:sqlserver://ZHENG\\SQLEXPRESS:1433;DatabaseName=TPCH";
      String connectDB="jdbc:sqlserver://ZHENG\\FAILFAST:2834;DatabaseName=TPCH";
      //String connectDB="jdbc:sqlserver://localhost:1433;DatabaseName=TPCH";
//数据源  ！！！！注意若出现加载或者连接数据库失败一般是这里出现问题
     // 我将在下面详述
      try {
    //加载数据库引擎，返回给定字符串名的类
          Class.forName(JDriver);
      }catch(ClassNotFoundException e)
      {
       //e.printStackTrace();
          System.out.println("加载数据库引擎失败");
          System.exit(0);
      }     
      System.out.println("数据库驱动成功");
      
      try {
          String user="sa";                                    
   //这里只要注意用户名密码不要写错即可
          String password="1214";
          Connection con=DriverManager.getConnection(connectDB,user,password);
//连接数据库对象
          System.out.println("连接数据库成功");
          Statement stmt=con.createStatement();
          Statement stmt2=con.createStatement();
//创建SQL命令对象
          
           
           // Counting the row numbers of RDS
           Binarysearch DB2 = new Binarysearch();

                      
           //Test query
           //String Testquery= "SELECT DISTINCT C_CUSTKEY, C_NAME, C_ACCTBAL, N_NAME, C_ADDRESS, C_PHONE, C_COMMENT, L_EXTENDEDPRICE, L_DISCOUNT, O_ORDERDATE, L_RETURNFLAG   FROM  OutCUSTOMER, OutORDERS, OutLINEITEM, OutNation   WHERE C_CUSTKEY = O_CUSTKEY   AND L_ORDERKEY = O_ORDERKEY  AND C_NATIONKEY = N_NATIONKEY";
           

           
           ResultSet rs;
           
           //why this number doesnt change doesnt matter
           //int[] BuildDbLogs=new int[17810];
           
           
           //set the right timestampt
//           int IndexSolution = 13598;
           
           int IndexSolution = 95;
           
           System.out.println("The Test right answer IndexSolution of RDS is " + IndexSolution);
           
           //Get the OutNumrows on RDS
           
/*         int m0=0; //customer
           int m1=0; //lineitem
           int m2=0; //orders
           int m3=0; //Nation
*/           
           String TestExeQuery;

           
           String str0 = "SELECT COUNT(*) FROM(";           
           String Testquery= "SELECT C_CUSTKEY, C_NAME, C_ACCTBAL, N_NAME, C_ADDRESS, C_PHONE, C_COMMENT, L_EXTENDEDPRICE, L_DISCOUNT, O_ORDERDATE, L_RETURNFLAG   FROM  TSCUSTOMER, TSORDERS, TSLINEITEM, TSNation WHERE C_CUSTKEY = O_CUSTKEY   AND L_ORDERKEY = O_ORDERKEY  AND C_NATIONKEY = N_NATIONKEY";
           String str1 = " AND TSCUSTOMER.TS < " + Integer.toString(IndexSolution);
           String str2 = " AND TSORDERS.TS < " + Integer.toString(IndexSolution);
           String str3 = " AND TSLINEITEM.TS < " + Integer.toString(IndexSolution);
           String str4 = " AND TSNATION.TS < " + Integer.toString(IndexSolution);
           String strend = ")As ttbl";
           
//           String Minushead = " Select * from Rout16998 Except ";
//           String Minusend = " Except select * from Rout1598 ";
           
            String Minushead = " Select * from Rout95 Except ";
            String Minusend = " Except select * from Rout95 ";
           
           String RoutMinusQhead = Minushead + Testquery; // need to add more str1...
           String QminusRout;
           
           
           //Get NumOutRows and Rout
           //NumOutRows
           TestExeQuery = str0+Testquery+ str1+str2+str3+str4+strend;
           
            
               rs=stmt.executeQuery(TestExeQuery);
            
               int NumOutRows=0;
               
               while(rs.next()){
            	   NumOutRows=rs.getInt(1);
            	   //System.out.println(NumOutRows);
               }
           //NumOutRows
               
           
           //Get exactly what they are
               String Outquery= Testquery+str1+str2+str3+str4;
               ResultSet rsOut=stmt2.executeQuery(Outquery);
           //Get exactly what they are
           
           
               
               
           //**********************************************************************************************************************************
               //Binary search need to updeted augments: 
               //1. Rout5k in the String valuable; and the corresponding Indexsolution variable; 2 + 1 places need to ba changed
               //2. Indexsolution to se the right timestampt
               //3. int partitionRange
               //This wont work for the timestamp in the last chunk -- fix it later
               
           String query;
           
           int SameNum=0;
           int middle = -1;//find the RDS
           
           
           String ClearData = "TRUNCATE TABLE TSCUSTOMER1";
           stmt.executeUpdate(ClearData);
           ClearData = "TRUNCATE TABLE TSLINEITEM1";
           stmt.executeUpdate(ClearData);
           ClearData = "TRUNCATE TABLE TSORDERS1";
           stmt.executeUpdate(ClearData);
           ClearData = "TRUNCATE TABLE TSNATION1";
           stmt.executeUpdate(ClearData);

           
           //1. Counting Rows Strategy
           long startTimeT = System.currentTimeMillis();
           long startTime = System.currentTimeMillis();
           
           String loadq = " insert into TSCUSTOMER1 select * from TSCUSTOMER";
           stmt.executeUpdate(loadq);
           loadq = "insert into TSORDERS1 select * from TSORDERS";
           stmt.executeUpdate(loadq);
           loadq = "insert into TSNATION1 select * from TSNATION";
           stmt.executeUpdate(loadq);
           loadq = "insert into TSLINEITEM1 select * from TSLINEITEM";
           stmt.executeUpdate(loadq);

           
   
           long sqltime=0;
           long oncesqltime=0;
           
           int partitionRange = 18000;
           //17810 maximum in ts lineitem
           
           for(int i = partitionRange; i < 17810; i = i + partitionRange){
   	 		   //The last timestamp is 27603
        	   
        	
   	 		   
   	 		str1 = " AND TSCUSTOMER.TS < " + Integer.toString(i);
            str2 = " AND TSORDERS.TS < " + Integer.toString(i);
            str3 = " AND TSLINEITEM.TS < " + Integer.toString(i);
            str4 = " AND TSNATION.TS < " + Integer.toString(i);
   	 		   
            
            
   	 		   TestExeQuery= RoutMinusQhead + str1+str2+str3+str4;
   	 		   
   	 		   
   	           long sqlstartTime = System.currentTimeMillis();
               rs=stmt.executeQuery(TestExeQuery);
               long sqendTime = System.currentTimeMillis();
               
               oncesqltime = sqendTime-sqlstartTime;
               sqltime = sqltime+oncesqltime;
               
               //System.out.println("Interesting: "+ sqltime);
               
 
               
               int NumTestRows=0;
               
               
               
               if(!rs.next()){
            	   //Binary search here in this chunk
            	   
            	   // need to check 'low' first; high must not be the RDS
            	   int low = i - partitionRange;
            	   int high = i; 
            	   
            	   while(high >= low){
            		   
            		   middle = (low+high)/2;
            		   
            		   str1 = " AND TSCUSTOMER.TS < " + Integer.toString(middle);
                       str2 = " AND TSORDERS.TS < " + Integer.toString(middle);
                       str3 = " AND TSLINEITEM.TS < " + Integer.toString(middle);
                       str4 = " AND TSNATION.TS < " + Integer.toString(middle);
            		   
            		   QminusRout = Testquery +str1+str2+str3+str4+Minusend;
            		   
            		   ResultSet rs1 = stmt.executeQuery(QminusRout);
            		   
            		   if(rs1.next())
            			   high = middle - 1;//go back
            		   
            		   else{
            		   String RoutMinusQ = RoutMinusQhead + str1+str2+str3+str4;            		   
            		   ResultSet rs2 = stmt.executeQuery(RoutMinusQ);
            		   if(rs2.next())  
            			   low = middle + 1;
            		   else
            			   break;
            		   }
            		   //rs q - rout
            		   
            		               		   
            	   }
            		               	   
            	   break;
            	   
               }
   	 		   
   	 		   
   	 		   if(i%1000==0){
   	 			long endTime   = System.currentTimeMillis();
  	 			long totalTime = endTime - startTime;
  	 			System.out.println("The running time in the step " + i + " Counting Rows Strategy is " + totalTime + " The part running query in SQL server "+ sqltime);
  	 			startTime = System.currentTimeMillis();
  	 			sqltime = 0;
   	 			//System.out.println(i);
   	 		   }
   	 		   
   	 		 //i = i + partitionRange;
   	 		   
   	 	   }
           
           
           
           long endTimeT   = System.currentTimeMillis();
           long totalTime = endTimeT - startTimeT;
           System.out.println("The running time for Binarysearch Strategy is "+totalTime);
           
		   System.out.println("Find out the RDS and the time step is "+ middle);


           
           System.out.println("Running Successfully");
           
           
           
       
       //关闭连接
       stmt.close();//关闭命令对象连接
       con.close();//关闭数据库连接
      }catch(SQLException e){
       e.printStackTrace();
       System.out.println(e.getErrorCode());
       //System.out.println("数据库连接错误");
       System.out.println("Wrong catch");
       System.exit(0);       
      }//For catch
      
 }//For main string args

}//For class Database

