import java.util.*;
import java.sql.*;
import java.io.*;


public class ViewRQ{ /**changed the class name here for the Java file**/

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
	

	public Long median(ArrayList<Long> m) {
	    int middle = m.size()/2;
	    if (m.size()%2 == 1) {
	        return m.get(middle);
	    } else {
	        return (m.get(middle-1) + m.get(middle)) / 2 ;
	    }
	} 
	
	
	public Long TruncatedMean(ArrayList<Long> m) {
	    int percentage = 2;	//20%
		int start = percentage;
	    int finish = m.size()-percentage;
	    Long sum = 0L;
	    //long sum = 0;
	    for (int i = start; i < finish; i++) 
	        sum += m.get(i);
	    
	        return sum/(finish - start);
	}
	
	
	public Long WinMean(ArrayList<Long> m) {
	    int percentage = 2;	//20%
		int start = percentage;
	    int finish = m.size()-percentage;
	    Long sum = 0L;
	    //long sum = 0;
	    
	    for (int i = 0; i < m.size(); i++) 
	        if(i<start)
	    	sum += m.get(start);
	        else if(i >= finish)
	        sum += m.get(finish-1);
	        else
	        sum += m.get(i);
	    
	        return sum/m.size();
	}
	
	
	public static void main(String[] args) {
      String JDriver="com.microsoft.sqlserver.jdbc.SQLServerDriver";
//SQL数据库引擎
      //String connectDB="jdbc:sqlserver://ZHENG\\SQLEXPRESS:1433;DatabaseName=TPCH";
      String connectDB="jdbc:sqlserver://ZHENG\\FAILFAST:2834;DatabaseName=TPCH2";
      //String connectDB="jdbc:sqlserver://localhost:1433;DatabaseName=TPCH";
//数据源  ！！！！注意若出现加载或者连接数据库失败一般是这里出现问题
     // 我将在下面详述
      try {
    //加载数据库引擎，返回给定字符串名的类
          Class.forName(JDriver);
      }catch(ClassNotFoundException e)
      {
       //e.printStackTrace();
          System.out.println("error connecting.");
          System.exit(0);
      }     
      System.out.println("connected");
      
      try {
          String user="sa";                                    
   //这里只要注意用户名密码不要写错即可
          String password="1214";
          Connection con=DriverManager.getConnection(connectDB,user,password);
//连接数据库对象
          System.out.println("user verified");
          Statement stmt=con.createStatement();
          Statement stmt2=con.createStatement();
          
          try{
        	  File writename = new File("E:\\CODING FILES\\java\\ViewRQ.txt"); // 相对路径，如果没有则要建立一个新的output。txt文件  
              writename.createNewFile(); // 创建新文件  
              BufferedWriter out = new BufferedWriter(new FileWriter(writename));  
              
          
           
           // Counting the row numbers of RDS
           ViewRQ ViewInstance = new ViewRQ();
//**changed here **//
                      
           
           
           ResultSet rs;
           
           //why this number doesnt change doesnt matter
           //int[] BuildDbLogs=new int[17810];
           
           
           //set the right timestampt
//           int IndexSolution = 13598;
           int[] IndexSol = {8253, 41765, 81098, 128977, 153736};
           int[] partitionSize = {1000, 5000, 10000, 50000, 170000};
           
        
           
           for(int qq = 0; qq < 4; qq++)
               for (int ii = 0; ii < 1; ii++)
            	   for(int pp = 1; pp < 2; pp++)
            		   {
               
            		   double AverageTotaltime = 0;
                       int RepeatNumbers = 10;
                       int countLinearS = -1;
                       int countBinaryS = -1;
                       long AverageLineartime = 0;
                       long AverageBinarytime = 0;
           //---------------------------------------------------------------------------------------------------------------------------
           //---------------------------------------------------------------------------------------------------------------------------
           int IndexSolution = IndexSol[ii];
           //8253, 41765, 81098, 128977, 153736
           int partitionRange = partitionSize[pp];
           int QueryIndex = qq;
           
           ArrayList<Long> TimeList = new ArrayList<Long>();
           ArrayList<Long> TimeListLinear = new ArrayList<Long>();
           ArrayList<Long> TimeListBinary = new ArrayList<Long>();           
           
           String[] FailQuery = new String[10];
    	   //FailQuery[0] is the correct query
               FailQuery[0] = "SELECT C_CUSTKEY, C_NAME, C_ACCTBAL , N_NAME , C_ADDRESS , C_PHONE , C_COMMENT , L_EXTENDEDPRICE, L_DISCOUNT , O_ORDERDATE , L_RETURNFLAG , TSCUSTOMER"+Integer.toString(partitionRange)+".TS as TSCUSTOMER"+Integer.toString(partitionRange)+"TS, TSORDERS"+Integer.toString(partitionRange)+".TS as TSORDERS"+Integer.toString(partitionRange)+"TS , TSLINEITEM"+Integer.toString(partitionRange)+".TS as TSLINEITEM"+Integer.toString(partitionRange)+"TS,  TSNATION"+Integer.toString(partitionRange)+".TS as TSNATION"+Integer.toString(partitionRange)+"TS FROM  TSCUSTOMER"+Integer.toString(partitionRange)+", TSORDERS"+Integer.toString(partitionRange)+", TSLINEITEM"+Integer.toString(partitionRange)+", TSNation"+Integer.toString(partitionRange)+" WHERE C_CUSTKEY = O_CUSTKEY   AND L_ORDERKEY = O_ORDERKEY  AND C_NATIONKEY = N_NATIONKEY";
    	   //FailQuery[1] is the correct query plus the constraint N_NATIONKEY < 23
               FailQuery[1] = "SELECT C_CUSTKEY, C_NAME, C_ACCTBAL , N_NAME , C_ADDRESS , C_PHONE , C_COMMENT , L_EXTENDEDPRICE, L_DISCOUNT , O_ORDERDATE , L_RETURNFLAG , TSCUSTOMER"+Integer.toString(partitionRange)+".TS as TSCUSTOMER"+Integer.toString(partitionRange)+"TS, TSORDERS"+Integer.toString(partitionRange)+".TS as TSORDERS"+Integer.toString(partitionRange)+"TS , TSLINEITEM"+Integer.toString(partitionRange)+".TS as TSLINEITEM"+Integer.toString(partitionRange)+"TS,  TSNATION"+Integer.toString(partitionRange)+".TS as TSNATION"+Integer.toString(partitionRange)+"TS FROM  TSCUSTOMER"+Integer.toString(partitionRange)+", TSORDERS"+Integer.toString(partitionRange)+", TSLINEITEM"+Integer.toString(partitionRange)+", TSNation"+Integer.toString(partitionRange)+" WHERE C_CUSTKEY = O_CUSTKEY   AND L_ORDERKEY = O_ORDERKEY  AND C_NATIONKEY = N_NATIONKEY AND N_NATIONKEY < 23 ";
              //FailQuery[2] is the correct query plus the constraint C_CUSTKEY < 149983
    	   FailQuery[2] = "SELECT C_CUSTKEY, C_NAME, C_ACCTBAL , N_NAME , C_ADDRESS , C_PHONE , C_COMMENT , L_EXTENDEDPRICE, L_DISCOUNT , O_ORDERDATE , L_RETURNFLAG , TSCUSTOMER"+Integer.toString(partitionRange)+".TS as TSCUSTOMER"+Integer.toString(partitionRange)+"TS, TSORDERS"+Integer.toString(partitionRange)+".TS as TSORDERS"+Integer.toString(partitionRange)+"TS , TSLINEITEM"+Integer.toString(partitionRange)+".TS as TSLINEITEM"+Integer.toString(partitionRange)+"TS,  TSNATION"+Integer.toString(partitionRange)+".TS as TSNATION"+Integer.toString(partitionRange)+"TS FROM  TSCUSTOMER"+Integer.toString(partitionRange)+", TSORDERS"+Integer.toString(partitionRange)+", TSLINEITEM"+Integer.toString(partitionRange)+", TSNation"+Integer.toString(partitionRange)+" WHERE C_CUSTKEY = O_CUSTKEY   AND L_ORDERKEY = O_ORDERKEY  AND C_NATIONKEY = N_NATIONKEY AND C_CUSTKEY < 149983";
    	   //FailQuery[1] is the correct query minus the constraint C_NATIONKEY=N_NATIONKEY
               FailQuery[3] = "SELECT C_CUSTKEY, C_NAME, C_ACCTBAL , N_NAME , C_ADDRESS , C_PHONE , C_COMMENT , L_EXTENDEDPRICE, L_DISCOUNT , O_ORDERDATE , L_RETURNFLAG , TSCUSTOMER"+Integer.toString(partitionRange)+".TS as TSCUSTOMER"+Integer.toString(partitionRange)+"TS, TSORDERS"+Integer.toString(partitionRange)+".TS as TSORDERS"+Integer.toString(partitionRange)+"TS , TSLINEITEM"+Integer.toString(partitionRange)+".TS as TSLINEITEM"+Integer.toString(partitionRange)+"TS,  TSNATION"+Integer.toString(partitionRange)+".TS as TSNATION"+Integer.toString(partitionRange)+"TS FROM  TSCUSTOMER"+Integer.toString(partitionRange)+", TSORDERS"+Integer.toString(partitionRange)+", TSLINEITEM"+Integer.toString(partitionRange)+", TSNation"+Integer.toString(partitionRange)+" WHERE C_CUSTKEY = O_CUSTKEY";


           String str0 = "SELECT COUNT(*) FROM(";
           
           //**********************************The part C that is different to the BinarySearch file*********************************
           //**********************************The part 1.F 1000--100*********************************

           //wrong testquery//String Testquery= "SELECT C_CUSTKEY, C_NAME, C_ACCTBAL, N_NAME, C_ADDRESS, C_PHONE, C_COMMENT, L_EXTENDEDPRICE, L_DISCOUNT, O_ORDERDATE, L_RETURNFLAG   FROM  TSCUSTOMER"+Integer.toString(partitionRange)+", TSORDERS"+Integer.toString(partitionRange)+", TSLINEITEM"+Integer.toString(partitionRange)+", TSNation"+Integer.toString(partitionRange)+" WHERE C_CUSTKEY = O_CUSTKEY   AND L_ORDERKEY = O_ORDERKEY  AND C_NATIONKEY = N_NATIONKEY";
           //String Testquery= "SELECT C_CUSTKEY, C_NAME, C_ACCTBAL , N_NAME , C_ADDRESS , C_PHONE , C_COMMENT , L_EXTENDEDPRICE, L_DISCOUNT , O_ORDERDATE , L_RETURNFLAG , TSCUSTOMER1000 .TS as TSCUSTOMER1000TS, TSORDERS1000.TS as TSORDERS1000TS , TSLINEITEM1000.TS as TSLINEITEM1000TS,  TSNATION1000.TS as TSNATION1000TS FROM  TSCUSTOMER"+Integer.toString(partitionRange)+", TSORDERS"+Integer.toString(partitionRange)+", TSLINEITEM"+Integer.toString(partitionRange)+", TSNation"+Integer.toString(partitionRange)+" WHERE C_CUSTKEY = O_CUSTKEY   AND L_ORDERKEY = O_ORDERKEY  AND C_NATIONKEY = N_NATIONKEY";
           //String Testquery= "SELECT C_CUSTKEY, C_NAME, C_ACCTBAL , N_NAME , C_ADDRESS , C_PHONE , C_COMMENT , L_EXTENDEDPRICE, L_DISCOUNT , O_ORDERDATE , L_RETURNFLAG , TSCUSTOMER3000 .TS as TSCUSTOMER3000TS, TSORDERS3000.TS as TSORDERS3000TS , TSLINEITEM3000.TS as TSLINEITEM3000TS,  TSNATION3000.TS as TSNATION3000TS FROM  TSCUSTOMER"+Integer.toString(partitionRange)+", TSORDERS"+Integer.toString(partitionRange)+", TSLINEITEM"+Integer.toString(partitionRange)+", TSNation"+Integer.toString(partitionRange)+" WHERE C_CUSTKEY = O_CUSTKEY   AND L_ORDERKEY = O_ORDERKEY  AND C_NATIONKEY = N_NATIONKEY";
           String Testquery2= "SELECT C_CUSTKEY, C_NAME, C_ACCTBAL , N_NAME , C_ADDRESS , C_PHONE , C_COMMENT , L_EXTENDEDPRICE, L_DISCOUNT , O_ORDERDATE , L_RETURNFLAG FROM Q WHERE ";
           String Testquery = FailQuery[QueryIndex];
           //   String Testquery= "SELECT C_CUSTKEY, C_NAME, C_ACCTBAL , N_NAME , C_ADDRESS , C_PHONE , C_COMMENT , L_EXTENDEDPRICE, L_DISCOUNT , O_ORDERDATE , L_RETURNFLAG , TSCUSTOMER"+Integer.toString(partitionRange)+".TS as TSCUSTOMER"+Integer.toString(partitionRange)+"TS, TSORDERS"+Integer.toString(partitionRange)+".TS as TSORDERS"+Integer.toString(partitionRange)+"TS , TSLINEITEM"+Integer.toString(partitionRange)+".TS as TSLINEITEM"+Integer.toString(partitionRange)+"TS,  TSNATION"+Integer.toString(partitionRange)+".TS as TSNATION"+Integer.toString(partitionRange)+"TS FROM  TSCUSTOMER"+Integer.toString(partitionRange)+", TSORDERS"+Integer.toString(partitionRange)+", TSLINEITEM"+Integer.toString(partitionRange)+", TSNation"+Integer.toString(partitionRange)+" WHERE C_CUSTKEY = O_CUSTKEY   AND L_ORDERKEY = O_ORDERKEY  AND C_NATIONKEY = N_NATIONKEY";
         
           //           String str1 = " AND TSCUSTOMER.TS < " + Integer.toString(IndexSolution);
//           String str2 = " AND TSORDERS.TS < " + Integer.toString(IndexSolution);
//           String str3 = " AND TSLINEITEM.TS < " + Integer.toString(IndexSolution);
//           String str4 = " AND TSNATION.TS < " + Integer.toString(IndexSolution);
           String str1 = " AND TSCUSTOMERTS < " + Integer.toString(IndexSolution);
           String str2 = " AND TSORDERSTS < " + Integer.toString(IndexSolution);
           String str3 = " AND TSLINEITEMTS < " + Integer.toString(IndexSolution);
           String str4 = " AND TSNATIONTS < " + Integer.toString(IndexSolution);
           String strend = ")As ttbl";
           
           
           String qView = "CREATE VIEW Q AS ";
           String qViewCreate = qView + Testquery;
           String DropView = " DROP VIEW Q"; // need to be changed *****************************
           stmt.executeUpdate(DropView);
           stmt.executeUpdate(qViewCreate);

           boolean FlagWrongQuery  = false; 
           
           for(int average=0; average < RepeatNumbers; average++){
          
        	   stmt.executeUpdate(DropView);  
          
           
           System.out.println("The Test right answer IndexSolution of RDS is " + IndexSolution);
           
           
           //**********************************The part 1.A that is 1000 to 100*********************************
           String ClearData = "TRUNCATE TABLE TSCUSTOMER"+Integer.toString(partitionRange);
           stmt.executeUpdate(ClearData);
           ClearData = "TRUNCATE TABLE TSLINEITEM"+Integer.toString(partitionRange);
           stmt.executeUpdate(ClearData);
           ClearData = "TRUNCATE TABLE TSORDERS"+Integer.toString(partitionRange);
           stmt.executeUpdate(ClearData);
           ClearData = "TRUNCATE TABLE TSNATION"+Integer.toString(partitionRange);
           stmt.executeUpdate(ClearData);
           
           String TestExeQuery;

          
           
//           String Minushead = " Select * from Rout13598 Except ";
//           String Minusend = " Except select * from Rout13598 ";
           
//            String Minushead = " Select * from Rout13598 Except ";
//            String Minusend = " Except select * from Rout13598 ";
           
           String Minushead = " Select * from Rout"+Integer.toString(IndexSolution)+" Except ";
           String Minusend = " Except select * from Rout"+Integer.toString(IndexSolution);
           
           String RoutMinusQhead = Minushead + Testquery2; // need to add more str1...
           String QminusRout;
           
           
              
           //**********************************************************************************************************************************
               //Binary search need to updeted augments: 
               //1. Rout5k in the String valuable; and the corresponding Indexsolution variable; 2 + 1 places need to ba changed
               //2. Indexsolution to se the right timestampt
               //3. int partitionRange
               //This wont work for the timestamp in the last chunk -- fix it later
               
           String query;
           
           int SameNum=0;
           int middle = -1;//find the RDS
       
           countLinearS = 0;
           
           //1. Counting Rows Strategy
           long startTimeT = System.currentTimeMillis();
           long startTime = System.currentTimeMillis();
           
           
           long BinaryTime = -1;
           long LinearTime = -1;
           
           
           long sqltime=0;
           long oncesqltime=0;
  		   //**********************************1.D 1000 to 100*********************************

           
           
           
           long LinearstartTimeT = System.currentTimeMillis();
           
           stmt.executeUpdate(qViewCreate);
           
           for(int i = partitionRange; i < 200001; i = i + partitionRange){
   	 		   //The last timestamp is 17810
        	
        	countLinearS++;   
        	
        	
        	
        	//**********************************The part A that is different to the BinarySearch file*********************************
      		//**********************************1.E 1000 to 100, 2 places!*********************************
        		
        	String Insertq = " INSERT INTO TSCUSTOMER"+Integer.toString(partitionRange)+" SELECT * FROM TSCUSTOMER WHERE TSCUSTOMER.TS >= " + Integer.toString(i-partitionRange) + " and TSCUSTOMER.TS < " + Integer.toString(i);
            stmt.executeUpdate(Insertq);
            Insertq = " INSERT INTO TSLINEITEM"+Integer.toString(partitionRange)+" SELECT * FROM TSLINEITEM WHERE TSLINEITEM.TS >= " + Integer.toString(i-partitionRange) + " and TSLINEITEM.TS < " + Integer.toString(i);
            stmt.executeUpdate(Insertq);
            Insertq = " INSERT INTO TSORDERS"+Integer.toString(partitionRange)+" SELECT * FROM TSORDERS WHERE TSORDERS.TS >= " + Integer.toString(i-partitionRange) + " and TSORDERS.TS < " + Integer.toString(i);
            stmt.executeUpdate(Insertq);
            Insertq = " INSERT INTO TSNATION"+Integer.toString(partitionRange)+" SELECT * FROM TSNATION WHERE TSNATION.TS >= " + Integer.toString(i-partitionRange) + " and TSNATION.TS < " + Integer.toString(i);
            stmt.executeUpdate(Insertq);
   	 		
        	//**********************************The part B that is different to the BinarySearch file*********************************
            //**********************************table name to 1000*********************************
   	    	//**********************************1.C 1000 to 100*********************************

            
   	 		//str1 = " AND TSCUSTOMER"+Integer.toString(partitionRange)+".TS < " + Integer.toString(i);
   	 		str1 = " TSCUSTOMER"+Integer.toString(partitionRange)+"TS < " + Integer.toString(i);
            str2 = " AND TSORDERS"+Integer.toString(partitionRange)+"TS < " + Integer.toString(i);
            str3 = " AND TSLINEITEM"+Integer.toString(partitionRange)+"TS < " + Integer.toString(i);
            str4 = " AND TSNATION"+Integer.toString(partitionRange)+"TS < " + Integer.toString(i);
   	 		   
            
            
   	 		   TestExeQuery= RoutMinusQhead + str1+str2+str3+str4;
   	 		   
   	 		   
   	           //long sqlstartTime = System.currentTimeMillis();
               rs=stmt.executeQuery(TestExeQuery);
               //long sqendTime = System.currentTimeMillis();
               
               //oncesqltime = sqendTime-sqlstartTime;
               //sqltime = sqltime+oncesqltime;
               
               //System.out.println("Interesting: "+ sqltime);
               
               
               
               int NumTestRows=0;
               
               if(rs.next()){ //if RoutMinusQ is not null
            	   
            	   QminusRout = Testquery2 +str1+str2+str3+str4+Minusend;        		   
        		   ResultSet rsWrongcase = stmt.executeQuery(QminusRout);
        		   if(rsWrongcase.next()){
        			   System.out.println("WrongQuery"); 
        			   FlagWrongQuery = true;
        			   break;
        		   }
               }
               
                              
               else{
            	   //Binary search here in this chunk
            	   long LinearEndTimeT = System.currentTimeMillis();
                   LinearTime = LinearEndTimeT - LinearstartTimeT;
                   AverageLineartime = AverageLineartime + LinearTime;
            	   
            	   long BinaryStartTimeT = System.currentTimeMillis();
            	   
            	   // need to check 'low' first; high must not be the RDS
            	   int low = i - partitionRange;
            	   int high = i; 
            	   
            	   countBinaryS = 0;
            	   
            	   while(high >= low){
            		   
            		   countBinaryS++;
            		   
            		   middle = (low+high)/2;
            		   
            		 //**********************************The part D that is different to the BinarySearch file*********************************
                     //**********************************table name to 1000*********************************
            		 //**********************************1.B 1000 to 100*********************************
            		   
            		   //str1 = " AND TSCUSTOMER"+Integer.toString(partitionRange)+".TS < " + Integer.toString(middle);
            		   str1 = " TSCUSTOMER"+Integer.toString(partitionRange)+"TS < " + Integer.toString(middle);
                       str2 = " AND TSORDERS"+Integer.toString(partitionRange)+"TS < " + Integer.toString(middle);
                       str3 = " AND TSLINEITEM"+Integer.toString(partitionRange)+"TS < " + Integer.toString(middle);
                       str4 = " AND TSNATION"+Integer.toString(partitionRange)+"TS < " + Integer.toString(middle);
            		   
            		   QminusRout = Testquery2 +str1+str2+str3+str4+Minusend;            		   
            		   ResultSet rs1 = stmt.executeQuery(QminusRout);
            		              		   
            		   boolean b1 = false;
            		   boolean b2 = false;
            		   if(rs1.next())
            			   b1 = true;
            		   
            		   String RoutMinusQ = RoutMinusQhead + str1+str2+str3+str4;            		   
            		   ResultSet rs2 = stmt.executeQuery(RoutMinusQ);
            		   
            		   if(rs2.next())
            			   b2 = true;
            		   
            		   if(b1&&b2){
            			   System.out.println("WrongQuery"); 
            			   FlagWrongQuery = true;
            			   break;
            		   }
            		   
            		   else if(b1&&(!b2))
            			   high = middle - 1;//go back
            		   
            		   else if(b2&&(!b1))  
            			   low = middle + 1;
            		   
            		   else
            			   break;
            		   
            		   
//            		   if(rs1.next()&&rs2.next()){
//            			   System.out.println("WrongQuery"); 
//            			   break;
//            		   }
//            		   
//            		   else if(rs1.next()&&(!rs2.next()))
//            			   high = middle - 1;//go back
//            		   
//            		   else if(rs2.next()&&(!rs1.next()))  
//            			   low = middle + 1;
//            		   
//            		   else
//            			   break;
            		   //rs q - rout
            		   
            		               		   
            	   }
            	   long BinaryEndTimeT = System.currentTimeMillis();
            	   BinaryTime = BinaryEndTimeT - BinaryStartTimeT;
            	   AverageBinarytime = AverageBinarytime + BinaryTime;
            	   
            	   break;
            	   
               }
   	 		   
               
   	 		   
   	 		   if(i%1000==0){
   	 			long endTime   = System.currentTimeMillis();
  	 			long totalTime = endTime - startTime;
  	 			System.out.println("The running time in the step " + i + " Counting Rows Strategy is " + totalTime + " The part running query in SQL server "+ sqltime);
  	 			startTime = System.currentTimeMillis();
  	 			//sqltime = 0;
   	 			//System.out.println(i);
   	 		   }
   	 		   
   	 		 //i = i + partitionRange;
   	 		   
   	 	   }
           
           
           
           long endTimeT   = System.currentTimeMillis();
           long totalTime = endTimeT - startTimeT;
           
           TimeList.add(totalTime);
           TimeListLinear.add(LinearTime);
           TimeListBinary.add(BinaryTime);
           
		   System.out.println("Find out the RDS and the time step is "+ middle);
		   System.out.println("************The number of linear Scan comparisons:************* " + countLinearS);
		   System.out.println("************The number of comparisons:************* " + countBinaryS);
		   System.out.println("The running time for Binarysearch Strategy is "+totalTime);
		   System.out.println("************The time of linear search:************* " + LinearTime);
		   System.out.println("************The time of binary search:************* " + BinaryTime);
           
           System.out.println("Running Successfully in the " + average + " time.");
           System.out.println("------------------------------------------------------------------------------------");
           AverageTotaltime = AverageTotaltime + totalTime;
           //AverageLineartime = AverageLineartime + LinearTime;
          
           }//average
           
           Collections.sort(TimeList);
           Collections.sort(TimeListLinear);
           Collections.sort(TimeListBinary);
           
           Long medianT = ViewInstance.median(TimeList);
           Long medianTL = ViewInstance.median(TimeListLinear);
           Long medianTB = ViewInstance.median(TimeListBinary);
           
           Long TruncatedmeanT = ViewInstance.TruncatedMean(TimeList);
           Long TruncatedmeanTL = ViewInstance.TruncatedMean(TimeListLinear);
           Long TruncatedmeanTB = ViewInstance.TruncatedMean(TimeListBinary);
                      
           Long WinMeanT = ViewInstance.WinMean(TimeList);
           Long WinMeanTL = ViewInstance.WinMean(TimeListLinear);
           Long WinMeanTB = ViewInstance.WinMean(TimeListBinary);
           
//         System.out.println("*****************************************Mean**********************************************");
//         System.out.println("The average time is: " + AverageTotaltime/RepeatNumbers);
//         System.out.println("The average time of linear search is: " + AverageLineartime/RepeatNumbers);
//         System.out.println("The average time of binary search is: " + AverageBinarytime/RepeatNumbers);
//         
//         System.out.println("*****************************************Median**********************************************");
//         System.out.println("The average time is: " + medianT);
//         System.out.println("The average time of linear search is: " + medianTL);
//         System.out.println("The average time of binary search is: " + medianTB);
//         
//         System.out.println("*****************************************Truncatedmean**********************************************");
//         System.out.println("The average time is: " + TruncatedmeanT);
//         System.out.println("The average time of linear search is: " + TruncatedmeanTL);
//         System.out.println("The average time of binary search is: " + TruncatedmeanTB);
//         
//         System.out.println("*****************************************WinsorizedMean**********************************************");
//         System.out.println("The average time is: " + WinMeanT);
//         System.out.println("The average time of linear search is: " + WinMeanTL);
//         System.out.println("The average time of binary search is: " + WinMeanTB);
         
//         System.out.println("***************************************************************************************");
//         System.out.println("The average time is: " + AverageTotaltime/RepeatNumbers);
//         System.out.println("The average time is: " + medianT);
//         System.out.println("The average time is: " + TruncatedmeanT);
//         System.out.println("The average time is: " + WinMeanT);
//         System.out.println("***************************************************************************************");
//         System.out.println("The average time of linear search is: " + AverageLineartime/RepeatNumbers);
//         System.out.println("The average time of linear search is: " + medianTL);
//         System.out.println("The average time of linear search is: " + TruncatedmeanTL);
//         System.out.println("The average time of linear search is: " + WinMeanTL);
//         System.out.println("***************************************************************************************");
//         System.out.println("The average time of binary search is: " + AverageBinarytime/RepeatNumbers);   
//         System.out.println("The average time of binary search is: " + medianTB);       
//         System.out.println("The average time of binary search is: " + TruncatedmeanTB);            
//         System.out.println("The average time of binary search is: " + WinMeanTB);
           
           System.out.println("***************************************************************************************");
           System.out.println(AverageTotaltime/RepeatNumbers);
           System.out.println(medianT);
           System.out.println(TruncatedmeanT);
           System.out.println(WinMeanT);
           System.out.println("***************************************************************************************");
           System.out.println(AverageLineartime/RepeatNumbers);
           System.out.println(medianTL);
           System.out.println(TruncatedmeanTL);
           System.out.println(WinMeanTL);
           System.out.println("***************************************************************************************");
           System.out.println(AverageBinarytime/RepeatNumbers);   
           System.out.println(medianTB);       
           System.out.println(TruncatedmeanTB);            
           System.out.println(WinMeanTB);
           
           
       out.write("***************************RDS: "+ IndexSolution + "; partitionSize: " + partitionRange+" ***************************"+"\r\n");
           
           if(FlagWrongQuery)
           out.write("This is a wrong query");
           out.write("\r\n");
           
		   //out.write("************The number of linear Scan comparisons:************* " + countLinearS +"\r\n");
		   //out.write("************The number of comparisons:************* " + countBinaryS +"\r\n");
           
           out.write(AverageTotaltime/RepeatNumbers+"\r\n"); // \r\n即为换行
           out.write(medianT+"\r\n");
           out.write(TruncatedmeanT+"\r\n");
           out.write(WinMeanT+"\r\n");
           
           out.write("\r\n");
           
           out.write(AverageLineartime/RepeatNumbers+"("+countLinearS+")"+"\r\n"); // \r\n即为换行
           out.write(medianTL+"\r\n");
           out.write(TruncatedmeanTL+"\r\n");
           out.write(WinMeanTL+"\r\n");
           
           out.write("\r\n");
           
         
           out.write(AverageBinarytime/RepeatNumbers+"("+countBinaryS+")\r\n");
       
           out.write(medianTB+"\r\n");
           out.write(TruncatedmeanTB+"\r\n");
           out.write(WinMeanTB+"\r\n");
           
           out.flush(); // 把缓存区内容压入文件
           
            		   } //end bracket for the main iteration loop of the three loopings ii,pp,qq
             
           out.close(); // 最后记得关闭文件
           
     	 	 }catch(Exception e){
     	 		e.printStackTrace();  
     	 	 }   
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

