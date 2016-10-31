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
//SQL���ݿ�����
      String connectDB="jdbc:sqlserver://ZHENG\\SQLEXPRESS:1433;DatabaseName=TPCH";
      //String connectDB="jdbc:sqlserver://localhost:1433;DatabaseName=TPCH";
//����Դ  ��������ע�������ּ��ػ����������ݿ�ʧ��һ���������������
     // �ҽ�����������
      try {
    //�������ݿ����棬���ظ����ַ���������
          Class.forName(JDriver);
      }catch(ClassNotFoundException e)
      {
       //e.printStackTrace();
          System.out.println("�������ݿ�����ʧ��");
          System.exit(0);
      }     
      System.out.println("���ݿ������ɹ�");
      
      try {
          String user="sa";                                    
   //����ֻҪע���û������벻Ҫд����
          String password="1214";
          Connection con=DriverManager.getConnection(connectDB,user,password);
//�������ݿ����
          System.out.println("�������ݿ�ɹ�");
          Statement stmt=con.createStatement();
          Statement stmt2=con.createStatement();
//����SQL�������
          
           
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
        		String pathname = "E:\\CODING FILES\\java\\BuildDbLogs.txt"; // ����·�������·�������ԣ������Ǿ���·����д���ļ�ʱ��ʾ���·��  
        	    File filename = new File(pathname); // Ҫ��ȡ����·����input��txt�ļ�  
        	    InputStreamReader reader = new InputStreamReader(  
        	            new FileInputStream(filename)); // ����һ������������reader  
        	    BufferedReader br = new BufferedReader(reader); // ����һ�����������ļ�����ת�ɼ�����ܶ���������  
        	    String line = "0";  
        	    //line = br.readLine();
        	    
        	    int i=0;
        	    while ((line = br.readLine()) != null) {  
        	        ; // һ�ζ���һ������
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
        		String pathname = "E:\\CODING FILES\\java\\IndexSolution.txt"; // ����·�������·�������ԣ������Ǿ���·����д���ļ�ʱ��ʾ���·��  
        	    File filename = new File(pathname); // Ҫ��ȡ����·����input��txt�ļ�  
        	    InputStreamReader reader = new InputStreamReader(  
        	            new FileInputStream(filename)); // ����һ������������reader  
        	    BufferedReader br = new BufferedReader(reader); // ����һ�����������ļ�����ת�ɼ�����ܶ���������  
        	    String line = "0";  
        	    //line = br.readLine();
        	    
        	    int i=0;
        	    while ((line = br.readLine()) != null) {  
        	        ; // һ�ζ���һ������
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
        	   File writename = new File("E:\\CODING FILES\\java\\sqltime.txt"); // ���·�������û����Ҫ����һ���µ�output��txt�ļ�  
               writename.createNewFile(); // �������ļ�  
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
               out.write(oncesqltime+"\r\n"); // \r\n��Ϊ����  
               out.flush(); // �ѻ���������ѹ���ļ� 
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
           out.close(); // ���ǵùر��ļ�
           
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
          //������
           System.out.println("��ʼ������");
          //������SQL���
          //String query= "create table TABLE1(ID NCHAR(2),NAME NCHAR(10))";
           query= "create table TABLE1(ID NCHAR(2),NAME NCHAR(10))";
           stmt.executeUpdate(query);//ִ��SQL�������
           System.out.println("�����ɹ�");
              
           //��������
           System.out.println("��ʼ��������");
           String a1="INSERT INTO TABLE1 VALUES('1','���')";
                //��������SQL���
           String a2="INSERT INTO TABLE1 VALUES('2','ΰ��')";
           String a3="INSERT INTO TABLE1 VALUES('3','�Ÿ�')";
           stmt.executeUpdate(a1);//ִ��SQL�������
           stmt.executeUpdate(a2);   
           stmt.executeUpdate(a3);
           System.out.println("�������ݳɹ�");
           
           //��ȡ����
           System.out.println("��ʼ��ȡ����");
           //ResultSet rs=stmt.executeQuery("SELECT * FROM TABLE1");//����SQL����ѯ�����(����)
           ResultSet rs=stmt.executeQuery("SELECT * FROM TABLE1");
       //ѭ�����ÿһ����¼
       while(rs.next()){
        //���ÿ���ֶ�
    	   System.out.println(rs.getString("ID")+"\t"+rs.getString("NAME"));
       }
       System.out.println("��ȡ���");
       
       */
       
       //�ر�����
       stmt.close();//�ر������������
       con.close();//�ر����ݿ�����
      }catch(SQLException e){
       e.printStackTrace();
       System.out.println(e.getErrorCode());
       //System.out.println("���ݿ����Ӵ���");
       System.out.println("Wrong catch");
       System.exit(0);       
      }//For catch
      
 }//For main string args

}//For class Database

