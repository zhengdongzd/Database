import java.util.*;
import java.sql.*;
import java.io.*;


public class Database2{

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
      String connectDB="jdbc:sqlserver://ZHENG\\SQLEXPRESS:1433;DatabaseName=TPCH";
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
           Database2 DB2 = new Database2();
           //should be reset based on the txt file
           
           //Second Part Counting numbers
           //Test query
           
      
           String Testquery= "SELECT DISTINCT C_CUSTKEY, C_NAME, C_ACCTBAL, N_NAME, C_ADDRESS, C_PHONE, C_COMMENT, L_EXTENDEDPRICE, L_DISCOUNT, O_ORDERDATE, L_RETURNFLAG   FROM  OutCUSTOMER, OutORDERS, OutLINEITEM, OutNation   WHERE C_CUSTKEY = O_CUSTKEY   AND L_ORDERKEY = O_ORDERKEY  AND C_NATIONKEY = N_NATIONKEY";
           
           
           
           
           ResultSet rs;
           
           //why this number doesnt change doesnt matter
           int[] BuildDbLogs=new int[17810];
           
           try{
        		String pathname = "E:\\CODING FILES\\java\\BuildDbLogs.txt"; // 绝对路径或相对路径都可以，这里是绝对路径，写入文件时演示相对路径  
        	    File filename = new File(pathname); // 要读取以上路径的input。txt文件  
        	    InputStreamReader reader = new InputStreamReader(  
        	            new FileInputStream(filename)); // 建立一个输入流对象reader  
        	    BufferedReader br = new BufferedReader(reader); // 建立一个对象，它把文件内容转成计算机能读懂的语言  
        	    String line = "0";  
        	    //line = br.readLine();
        	    
        	    int i=0;
        	    while ((line = br.readLine()) != null) {  
        	        ; // 一次读入一行数据
        	        BuildDbLogs[i]=Integer.parseInt(line);
        	        /*
        	        if(i%1000==0)
        	        	System.out.println(BuildDbLogs[i]);
        	        */
        	        i++;
        	    }
        	    

        	    br.close();
        		}catch(Exception e){
        		 		e.printStackTrace();  
        		 	 }
           
           int IndexSolution=0;
           
           try{
        		String pathname = "E:\\CODING FILES\\java\\IndexSolution.txt"; // 绝对路径或相对路径都可以，这里是绝对路径，写入文件时演示相对路径  
        	    File filename = new File(pathname); // 要读取以上路径的input。txt文件  
        	    InputStreamReader reader = new InputStreamReader(  
        	            new FileInputStream(filename)); // 建立一个输入流对象reader  
        	    BufferedReader br = new BufferedReader(reader); // 建立一个对象，它把文件内容转成计算机能读懂的语言  
        	    String line = "0";  
        	    //line = br.readLine();
        	    
        	    int i=0;
        	    while ((line = br.readLine()) != null) {  
        	        ; // 一次读入一行数据
        	        IndexSolution=Integer.parseInt(line);
        	        /*
        	        if(i%1000==0)
        	        	System.out.println(BuildDbLogs[i]);
        	        */
        	        i++;
        	    }
        	    

        	    br.close();
        		}catch(Exception e){
        		 		e.printStackTrace();  
        		 	 }
           
           
           System.out.println("The IndexSolution of RDS is "+IndexSolution);
           
           //Get the OutNumrows on RDS
           
           int m0=0; //customer
           int m1=0; //lineitem
           int m2=0; //orders
           int m3=0; //Nation
           
           String TestExeQuery;
           
           for(int i=0;i<IndexSolution;i++){

   	 		   if(BuildDbLogs[i]==1){
   	 			m0++;
   	 		   }
   	 		   if(BuildDbLogs[i]==2){
   	 			m1++;
   	 		   }
   	 		   if(BuildDbLogs[i]==3){
	 			m2++;
	 		   }
   	 		   if(BuildDbLogs[i]==4){
   	 			m3++;
   	 		   }	
           }
           
           
           System.out.println("The RDS m0,m1,m2,m3 on RDS:"+m0+" "+m1+" "+m2+" "+m3);
           
           
           String str0 = "SELECT COUNT(*) FROM(";
           
           String str1 = " AND OUTCUSTOMER.ID < " + Integer.toString(m0);
           String str2 = " AND OutORDERS.ID < " + Integer.toString(m1);
           String str3 = " AND OutLINEITEM.ID < " + Integer.toString(m2);
           String str4 = " AND OUTNATION.ID < " + Integer.toString(m3);
   	 		
           String strend = ")As ttbl";
           
           TestExeQuery = str0+Testquery+ str1+str2+str3+str4+strend;
           
            
               rs=stmt.executeQuery(TestExeQuery);
            
               int NumOutRows=0;
               
               while(rs.next()){
            	   NumOutRows=rs.getInt(1);
            	   //System.out.println(NumOutRows);
               }
           //NumOutRows
               
               String Outquery= Testquery+ str1+str2+str3+str4;
               ResultSet rsOut=stmt2.executeQuery(Outquery);
           //Get exactly what they are
           
           //Get the OutNumrows on RDS ends//NumOutRows


           
           String query;
           

           int SameNum=0;
   	 	   
           
           //1. Counting Rows Strategy
           long startTimeT = System.currentTimeMillis();
           long startTime = System.currentTimeMillis();
           
           
           m0=0; //customer
           m1=0; //lineitem
           m2=0; //orders
           m3=0; //Nation
           
           long sqltime=0;
           long oncesqltime=0;
           
           /*
           try{
        	   File writename = new File("E:\\CODING FILES\\java\\sqltime.txt"); // 相对路径，如果没有则要建立一个新的output。txt文件  
               writename.createNewFile(); // 创建新文件  
               BufferedWriter out = new BufferedWriter(new FileWriter(writename));  
           */
           
           for(int i=0;i<BuildDbLogs.length;i++){
   	 		   
        	   
        	   //string fmt = "INSERT INTO %s SELECT * FROM LINEITEM WHERE ID = %s";
   	 		   if(BuildDbLogs[i]==1){
   	 			//System.out.println(BuildDbLogs[i+1]);
   	 			//query="INSERT INTO TestCUSTOMER SELECT * FROM CUSTOMER WHERE ID =" + Integer.toString(BuildDbLogs[i+1]);
   	 			//stmt.executeUpdate(query);
   	 			m0++;
   	 		   }
   	 		   if(BuildDbLogs[i]==2){
   	 			//System.out.println(BuildDbLogs[i+1]);
   	 			m1++;
   	 		   }
   	 		   if(BuildDbLogs[i]==3){
   	 			//System.out.println(BuildDbLogs[i+1]);
	 			m2++;
	 		   }
   	 		   if(BuildDbLogs[i]==4){
   	 			//System.out.println(BuildDbLogs[i+1]);
   	 			m3++;
   	 		   }	
   	 		   
   	 		str1 = " AND OUTCUSTOMER.ID < " + Integer.toString(m0);
            str2 = " AND OutORDERS.ID < " + Integer.toString(m1);
            str3 = " AND OutLINEITEM.ID < " + Integer.toString(m2);
            str4 = " AND OUTNATION.ID < " + Integer.toString(m3);
   	 		   
   	 		   TestExeQuery= str0+Testquery+ str1+str2+str3+str4+strend;
   	 		   
   	           long sqlstartTime = System.currentTimeMillis();
               rs=stmt.executeQuery(TestExeQuery);
               long sqendTime = System.currentTimeMillis();
               
               oncesqltime = sqendTime-sqlstartTime;
               sqltime = sqltime+oncesqltime;
               
               //System.out.println("Interesting: "+ sqltime);
               
               
               /*
               out.write(oncesqltime+"\r\n"); // \r\n即为换行  
               out.flush(); // 把缓存区内容压入文件 
               */ 
               
               int NumTestRows=0;
               
               while(rs.next()){
            	   NumTestRows=rs.getInt(1);
            	   //System.out.println(NumOutRows);
               }
   	 		   
               if(NumTestRows==NumOutRows){
            	   
            	   System.out.println("NumOutRows in compare loop "+ NumTestRows);
            	   
            	   SameNum++;
            	   
            	   String OutqueryIteration= Testquery+ str1+str2+str3+str4;
            	   ResultSet rsOutIteration=stmt.executeQuery(OutqueryIteration);
            	   
            	   rsOut=stmt2.executeQuery(Outquery);
            	   
                   if(DB2.compareResultSets(rsOut, rsOutIteration)){
            		   System.out.println("Find out the RDS and the time step is "+ i);
            		   System.out.println("m0,m1,m2,m3 on RDS:"+m0+" "+m1+" "+m2+" "+m3);
            		   break;
            		   
            	   }
            	   		
               }
               
               //if(NumTestRows>NumOutRows)
            	   //break;
               
               
               //if((i%DistributionTime==0)){
      	 			
      	 	   //	   }
                             
   	 		   //i++;
   	 		   
   	 		   if(i%2000==0){
   	 			long endTime   = System.currentTimeMillis();
  	 			long totalTime = endTime - startTime;
  	 			System.out.println("The running time in the step " + i + " Counting Rows Strategy is " + totalTime + " The part running query in SQL server "+ sqltime);
  	 			startTime = System.currentTimeMillis();
  	 			sqltime = 0;
   	 			//System.out.println(i);
   	 		   }
   	 		   
   	 		   
   	 		   
   	 	   }
           
           /* 
           out.close(); // 最后记得关闭文件
           
           }catch(Exception e){
      	 		e.printStackTrace();  
     	 	 }
           */
           
           long endTimeT   = System.currentTimeMillis();
           long totalTime = endTimeT - startTimeT;
           System.out.println("The running time for Counting Rows Strategy is "+totalTime);
           
           
           System.out.println("SameNum:"+SameNum);
           
           
           //2. hashmap method
           //Store the rds tables to the hashmap and counting the rows

           rs=stmt.executeQuery(Outquery);
           HashMap<Integer, Integer> hm = new HashMap<Integer, Integer>();
           int hmRows=0;
           
           while(rs.next()){
        	   int hmCUSTKEY=rs.getInt(1);
        	   
        	   /*
        	   if(hmRows<20)
        	   System.out.println(hmCUSTKEY);
        	   */
        	   
        	   if (hm.containsKey(hmCUSTKEY)) {
        		   hm.put(hmCUSTKEY, hm.get(hmCUSTKEY)+1);
               }
        	   
        	   else
               hm.put(hmCUSTKEY, 1);

               hmRows++;
           }
           
           
           //System.out.println("hmRows:"+hmRows);
           
           //Now we have the hashmap and the hmRows
           
           m0=0;
           m1=0;
           m2=0;
           m3=0;
           
           
           	   startTimeT = System.currentTimeMillis();
           	   startTime = System.currentTimeMillis();
           	   
           	   for(int i=0;i<BuildDbLogs.length;i++){
           		   
           	   //int DistributionTime = BuildDbLogs.length/5;
           		
   	 		   //string fmt = "INSERT INTO %s SELECT * FROM LINEITEM WHERE ID = %s";
   	 		   if(BuildDbLogs[i]==1){
   	 			m0++;
   	 		   }
   	 		   if(BuildDbLogs[i]==2){
   	 			m1++;
   	 		   }
   	 		   if(BuildDbLogs[i]==3){
   	 			m2++;
	 		   }
   	 		   if(BuildDbLogs[i]==4){
   	 			m3++;
   	 		   }	
   	 		   
   	 		   
   	 		str1 = " AND OUTCUSTOMER.ID < " + Integer.toString(m0);
            str2 = " AND OutORDERS.ID < " + Integer.toString(m1);
            str3 = " AND OutLINEITEM.ID < " + Integer.toString(m2);
            str4 = " AND OUTNATION.ID < " + Integer.toString(m3);
   	 		   
            String OutqueryIteration= Testquery+ str1+str2+str3+str4;

   	 		   
             
            ResultSet rsOutIteration=stmt.executeQuery(OutqueryIteration);
            
            //Bug1
            //This is so important that I forget this in the first time, in what condition I just create one rsOut then I run the DB2.compareResultSets(rsOutIteration, rsOut) again and again
            //which holds reOut in this function. And we move to next line each time. While we expect we could start from the first row at each iteration while it not.
            
            rsOut=stmt2.executeQuery(Outquery);
            	   
                   // Here the order of the rsOutIteration and  rsOut is DB2.compareResultSets(rsOutIteration, rsOut)
                   
            int counthm=0;
            
            while(rsOutIteration.next()){
            	if(hm.containsKey(rsOutIteration.getInt(1)))
            		;
            	else
            		break;
            	counthm++;
            }
            
            
            if(counthm==hmRows){
            	rsOutIteration=stmt.executeQuery(OutqueryIteration);
            	if(DB2.compareResultSets(rsOutIteration, rsOut)){
            		   System.out.println("Find out the RDS and the time step is "+ i);
            		   System.out.println("m0,m1,m2,m3 on RDS:"+m0+" "+m1+" "+m2+" "+m3);
            		   break;
            	   }
            }
            
   	 		   
   	 		   if(i%2000==0){
   	 			long endTime   = System.currentTimeMillis();
 	 			totalTime = endTime - startTime;
 	 			System.out.println("The running time in the step " + i + " Hashmap Strategy is " + totalTime);
 	 			startTime = System.currentTimeMillis();
   	 			//System.out.println(i);
   	 		   }
   	 		   
   	 	   }
   	 	   
           
           endTimeT   = System.currentTimeMillis();
           totalTime = endTimeT - startTimeT;
           System.out.println("The running time for Hashmap is "+totalTime);
           
           
           
           
           	   
           //3. Naive Method
      
           m0=0;
           m1=0;
           m2=0;
           m3=0;
           
           
           	   startTimeT = System.currentTimeMillis();
           	   startTime = System.currentTimeMillis();
           	   
           	   for(int i=0;i<BuildDbLogs.length;i++){
           		   
           	   //int DistributionTime = BuildDbLogs.length/5;
           		
   	 		   //string fmt = "INSERT INTO %s SELECT * FROM LINEITEM WHERE ID = %s";
   	 		   if(BuildDbLogs[i]==1){
   	 			m0++;
   	 		   }
   	 		   if(BuildDbLogs[i]==2){
   	 			m1++;
   	 		   }
   	 		   if(BuildDbLogs[i]==3){
   	 			m2++;
	 		   }
   	 		   if(BuildDbLogs[i]==4){
   	 			m3++;
   	 		   }	
   	 		   
   	 		   
   	 		str1 = " AND OUTCUSTOMER.ID < " + Integer.toString(m0);
            str2 = " AND OutORDERS.ID < " + Integer.toString(m1);
            str3 = " AND OutLINEITEM.ID < " + Integer.toString(m2);
            str4 = " AND OUTNATION.ID < " + Integer.toString(m3);
   	 		   
            String OutqueryIteration= Testquery+ str1+str2+str3+str4;

   	 		   
             
            ResultSet rsOutIteration=stmt.executeQuery(OutqueryIteration);
            
            //Bug1
            //This is so important that I forget this in the first time, in what condition I just create one rsOut then I run the DB2.compareResultSets(rsOutIteration, rsOut) again and again
            //which holds reOut in this function. And we move to next line each time. While we expect we could start from the first row at each iteration while it not.
            
            rsOut=stmt2.executeQuery(Outquery);
            	   
                   // Here the order of the rsOutIteration and  rsOut is DB2.compareResultSets(rsOutIteration, rsOut)
                   if(DB2.compareResultSets(rsOutIteration, rsOut)){
            		   System.out.println("Find out the RDS and the time step is "+ i);
            		   System.out.println("m0,m1,m2,m3 on RDS:"+m0+" "+m1+" "+m2+" "+m3);
            		   break;
            	   }
               
   	 		   
   	 		   if(i%2000==0){
   	 			long endTime   = System.currentTimeMillis();
 	 			totalTime = endTime - startTime;
 	 			System.out.println("The running time in the step " + i + " Counting Rows Strategy is " + totalTime);
 	 			startTime = System.currentTimeMillis();
   	 			//System.out.println(i);
   	 		   }
   	 		   
   	 	   }
   	 	   
           
           endTimeT   = System.currentTimeMillis();
           totalTime = endTimeT - startTimeT;
           System.out.println("The running time for Naive Method is "+totalTime);
           
           
           
           //3.

           
           System.out.println("Running Successfully");
           
           
           
           //System.out.println("NumCUSTOMER"+NumCUSTOMER);
          
          /*
          ////Original Code for creating databases
          //创建表
           System.out.println("开始创建表");
          //创建表SQL语句
          //String query= "create table TABLE1(ID NCHAR(2),NAME NCHAR(10))";
           query= "create table TABLE1(ID NCHAR(2),NAME NCHAR(10))";
           stmt.executeUpdate(query);//执行SQL命令对象
           System.out.println("表创建成功");
              
           //输入数据
           System.out.println("开始插入数据");
           String a1="INSERT INTO TABLE1 VALUES('1','旭哥')";
                //插入数据SQL语句
           String a2="INSERT INTO TABLE1 VALUES('2','伟哥')";
           String a3="INSERT INTO TABLE1 VALUES('3','张哥')";
           stmt.executeUpdate(a1);//执行SQL命令对象
           stmt.executeUpdate(a2);   
           stmt.executeUpdate(a3);
           System.out.println("插入数据成功");
           
           //读取数据
           System.out.println("开始读取数据");
           //ResultSet rs=stmt.executeQuery("SELECT * FROM TABLE1");//返回SQL语句查询结果集(集合)
           ResultSet rs=stmt.executeQuery("SELECT * FROM TABLE1");
       //循环输出每一条记录
       while(rs.next()){
        //输出每个字段
    	   System.out.println(rs.getString("ID")+"\t"+rs.getString("NAME"));
       }
       System.out.println("读取完毕");
       
       */
       
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

