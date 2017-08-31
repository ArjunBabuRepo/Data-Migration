import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.sforce.soap.enterprise.Connector;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.QueryResult;
import com.sforce.soap.enterprise.sobject.RecordType;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class Incremental_load2 {
	/*static final String USERNAME = "bi@synaptics.com";
	// static final String USERNAME = "bi@synaptics.com";
	static final String PASSWORD = "Syna@123OtF1EvybbSCkCExy77XradBCQ";
	// static final String PASSWORD = "snuv1k123";*/
	
	static String USERNAME1;
	static String USERNAME2;
	static String PASSWORD1;
	// static final String PASSWORD = "snuv1k123";
	static String PASSWORD2;
	static String LastRunTime;

	static EnterpriseConnection connection;

	public static void main(String[] args) {
		PropertiesFile();
		ConnectorConfig config = new ConnectorConfig();
		config.setUsername(USERNAME1);
		config.setPassword(PASSWORD1);
		config.setAuthEndpoint("https://synaptics.my.salesforce.com/services/Soap/c/36.0");
		try {
			connection = Connector.newConnection(config);

			System.out.println("Auth EndPoint: " + config.getAuthEndpoint());
			System.out.println("Service EndPoint: " + config.getServiceEndpoint());
			System.out.println("Username: " + config.getUsername());
			System.out.println("SessionId: " + config.getSessionId());
			doit();
			deleteOldRows();
			System.out.println("Update completed");
		} catch (Exception e) {
			System.out.println(e);
		} finally {
			if (connection != null) {
				try {
					connection.logout();
				} catch (ConnectionException e) {
					e.printStackTrace();
				}
			}

		}
	}
	
	
	
	public static Connection getConnection1() throws IOException{
			 
			final String URL = "jdbc:oracle:thin:@182.18.161.151:1523:AGDEV1";
			
		    /**
		     * In my case username is "root" *
		     */
		    final String USERNAME = USERNAME2;
			 
		    /**
		     * In my case password is "1234" *
		     */
		    final String PASSWORD = PASSWORD2;
		  
		  
		        Connection connection = null;
		 
		        try {
		        	
		        	
		            //Class.forName("com.mysql.jdbc.Driver");
		        	Class.forName("oracle.jdbc.driver.OracleDriver");
		        	
		        	//DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
		            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
		        } catch (Exception e) {
		            e.printStackTrace();
		            System.exit(1);
		        }
		 
		        return connection;
		    }
	
	
	
	public static  void PropertiesFile()
		{
			try {
				File file = new File("C:/Users/Chintamani/Desktop/Login/uinfo.properties");
				FileInputStream fileInput = new FileInputStream(file);
				Properties properties = new Properties();
				properties.load(fileInput);
				fileInput.close();

				Enumeration<Object> enuKeys = properties.keys();
				USERNAME1= properties.getProperty("username1");
				PASSWORD1 = properties.getProperty("password1");
				
				USERNAME2= properties.getProperty("username2");
				PASSWORD2= properties.getProperty("password2");
				LastRunTime=properties.getProperty("LastRunnedTime");
				
				
				
				
				
				/*while (enuKeys.hasMoreElements()) {
					String key = (String) enuKeys.nextElement();
					String value = properties.getProperty(key);
					System.out.println(key + ": " + value);
				}*/
			} 
			catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
	

	private static void deleteOldRows() {
		Connection destination = null;
		PreparedStatement preparedStatement1 = null;
		PreparedStatement preparedStatement2 = null;
		ResultSet resultSet1 = null;
		try {
			destination = getConnection();
			preparedStatement1 = destination.prepareStatement("select ID from RECORDTYPE");
			resultSet1 = preparedStatement1.executeQuery();
			while (resultSet1.next()) {
				String ID = resultSet1.getString("ID");
				System.out.println("Checking if ID = "+ID+" is deleted");
				QueryResult queryResults1 = connection.query("select Id from RecordType where  ID ='" + ID + "'");
				if (queryResults1.getSize() == 0) {
					System.out.println("Deleting old ID =" + ID);
					preparedStatement2 = destination.prepareStatement("delete from RECORDTYPE where ID ='" + ID + "'");
					preparedStatement2.executeUpdate();
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			if (preparedStatement2 != null) {
				try {
					preparedStatement2.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (resultSet1 != null) {
				try {
					resultSet1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (preparedStatement1 != null) {
				try {
					preparedStatement1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (destination != null) {
				try {
					destination.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

	}

	private static void doit() {
		Connection destination = null;
		PreparedStatement preparedStatement1 = null;
		PreparedStatement preparedStatement2 = null;
		PreparedStatement preparedStatement3 = null;
		ResultSet resultSet1 = null;
		ResultSet resultSet2 = null;
		try {
			destination = getConnection();
			preparedStatement1 = destination.prepareStatement("select max(LASTMODIFIEDDATE) from RecordType");
			resultSet1 = preparedStatement1.executeQuery();
			resultSet1.next();
			Date LASTMODIFIEDDATE = resultSet1.getDate("MAX(LASTMODIFIEDDATE)");
			System.out.println("LASTMODIFIEDDATE = " + LASTMODIFIEDDATE);

			QueryResult queryResults1 = connection.query("select Id from RecordType where DAY_ONLY(LastModifiedDate) >=" + LASTMODIFIEDDATE);
			if (queryResults1.getSize() > 0) {
				for (int i = 0; i < queryResults1.getRecords().length; i++) {
					RecordType c = (RecordType) queryResults1.getRecords()[i];
					String Id1 = c.getId();
					System.out.println("Id from source = " + Id1);

					preparedStatement2 = destination.prepareStatement("select ID from RECORDTYPE where ID ='" + Id1 + "'");
					resultSet2 = preparedStatement2.executeQuery();

					if (resultSet2.next()) {
						String ID = resultSet2.getString("ID");
						System.out.println("Deleting ID =" + ID);
						preparedStatement3 = destination.prepareStatement("delete from RECORDTYPE where ID ='" + ID + "'");
						preparedStatement3.executeUpdate();
						queryRecordType(ID);
					} else {
						queryRecordType(Id1);

					}

				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (preparedStatement3 != null) {
				try {
					preparedStatement3.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (resultSet2 != null) {
				try {
					resultSet2.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (preparedStatement2 != null) {
				try {
					preparedStatement2.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (resultSet1 != null) {
				try {
					resultSet1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (preparedStatement1 != null) {
				try {
					preparedStatement1.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (destination != null) {
				try {
					destination.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	
	
		private static void queryRecordType(String yeah) {
	   // System.out.println("Querying for the 5 newest Contacts...");
		Connection connection1 = null;
		PreparedStatement preparedStatement = null;
	    try {
	    	connection1 = getConnection();
			preparedStatement = connection1.prepareStatement("Truncate Table RECORDTYPE");
			preparedStatement.executeUpdate();
	      // query for the 5 newest contacts     

	      QueryResult queryResults = connection.query("Select  Id, Name,DeveloperName, NamespacePrefix, Description, BusinessProcessId,SobjectType,IsActive, CreatedById,CreatedDate,LastModifiedById, LastModifiedDate, SystemModstamp FROM RecordType where id = '"+ yeah + "'");
	      if (queryResults.getSize() > 0) {
	    	  for (int i=0;i<queryResults.getRecords().length;i++) 
	    	  {
	    		  RecordType c = (RecordType)queryResults.getRecords()[i];
	    		  String Id=c.getId();
	    		  
	    		  String Name=c.getName();
	    		  
	    		  String DeveloperName=c.getDeveloperName();
	    		  
	    		  String NamespacePrefix=c.getNamespacePrefix();
	    		  String  Description=c.getDescription();
	    		  String  BusinessProcessId=c.getBusinessProcessId();
	    		  String SobjectType=c.getSobjectType();
	    		  Boolean IsActive=c.getIsActive();
	    		  String CreatedById=c.getCreatedById();
	    		  
	    		  Calendar CreatedDate_1=c.getCreatedDate();
	    		  Timestamp  CreatedDate=getEmptyTimestamp( CreatedDate_1 );
	    		  
	    		  String LastModifiedById=c.getLastModifiedById();
	    		  
	    		  Calendar LastModifiedDate_1=c.getLastModifiedDate();
	    		  Timestamp LastModifiedDate=getEmptyTimestamp(LastModifiedDate_1);
	    		  
	    		  Calendar SystemModstamp_1=c.getSystemModstamp();
	    		  Timestamp SystemModstamp=getEmptyTimestamp(SystemModstamp_1);
	    		  
	    		 Insert_queryRecordType(Id,Name,DeveloperName,NamespacePrefix,Description, BusinessProcessId,SobjectType,IsActive,CreatedById,CreatedDate,LastModifiedById,LastModifiedDate,SystemModstamp);
	    		  System.out.println(""Inserting Id: "  +Id +"Name: " + Name+"DeveloperName: " +DeveloperName+"NamespacePrefix: " + NamespacePrefix+" Description: " +  Description+" "+"BusinessProcessId" +BusinessProcessId+"SobjectType: " +SobjectType+"IsActive: " +IsActive+
				  			"CreatedById: " +CreatedById+" CreatedDate"+ CreatedDate+"LastModifiedById"+LastModifiedById+"LastModifiedDate"+LastModifiedDate+"SystemModstamp: " +SystemModstamp);
	    		  
	    		  System.out.println();
	    	  }
	

	    }
	    }
	    catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
	    }
	
	
	public static void Insert_queryRecordType( String Id,String Name,String DeveloperName,String NamespacePrefix, String  Description,String  BusinessProcessId, 
			  String SobjectType,Boolean IsActive, String CreatedById, Timestamp  CreatedDate,String LastModifiedById,Timestamp LastModifiedDate,Timestamp SystemModstamp)
	{
		Connection target = null;
	    PreparedStatement preparedStatement = null;
	    
	    try{
	    	target = getConnection();
	    	preparedStatement =  target.prepareStatement("INSERT INTO RECORDTYPE(NAME,DEVELOPERNAME,NAMESPACEPREFIX,DESCRIPTION,BUSINESSPROCESSID,SOBJECTTYPE,ISACTIVE,CREATEDBYID,CREATEDDATE,LASTMODIFIEDBYID,LASTMODIFIEDDATE,SYSTEMMODSTAMP,ID)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)");
	    	   	
	    	preparedStatement.setString(1, Name);
	    	preparedStatement.setString(2,DeveloperName);
	    	preparedStatement.setString(3,NamespacePrefix);
	    	preparedStatement.setString(4,Description);
	    	preparedStatement.setString(5,BusinessProcessId);
	    	preparedStatement.setString(6,SobjectType);
	    	preparedStatement.setBoolean(7,IsActive);
	    	preparedStatement.setString(8,CreatedById);
	    	preparedStatement.setTimestamp(9,CreatedDate);
	    	preparedStatement.setString(10,LastModifiedById);
	    	preparedStatement.setTimestamp(11,LastModifiedDate);
	    	preparedStatement.setTimestamp(12,SystemModstamp);
	    	preparedStatement.setString(13,Id);
	    	
	    	preparedStatement.executeUpdate();
	    	
	}
	    catch(Exception e){
		   	 e.printStackTrace();
		   //	 sendEmail("Shipping Schedule Database Insertion Exception",e.getMessage());
		    }finally{
			   	 if (preparedStatement != null) {
			            try {
			                preparedStatement.close();
			            } catch (SQLException e) {
			                e.printStackTrace();
			            }
			        }

			        if (target != null) {
			            try {
			                target.close();
			            } catch (SQLException e) {
			                e.printStackTrace();
			            }
			        }

			   }
}

	

	public static Connection getConnection() throws IOException {

		final String URL = "jdbc:oracle:thin:@sjc1uvt-obi03.synaptics.com:1522:BITST12C";

		/**
		 * In my case username is "root" *
		 */
		final String USERNAME = "SFDC_SOURCE";

		/**
		 * In my case password is "1234" *
		 */
		final String PASSWORD = "syna1234";

		Connection connection = null;

		try {

			// Class.forName("com.mysql.jdbc.Driver");
			Class.forName("oracle.jdbc.driver.OracleDriver");

			// DriverManager.registerDriver(new
			// oracle.jdbc.driver.OracleDriver());
			connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		return connection;
	}

	

	public static Double xyz(Double aaa) {
		if (aaa == null) {
			Double a = 0.0;
			return a;

		} else {
			Double yyy = aaa;
			return yyy;
		}
	}

    public static Timestamp getEmptyTimestamp(Calendar dates){
       Timestamp formatted;
       if(dates==null){
    	   formatted=null;
    	   
       }else{
    	   formatted=new Timestamp(dates.getTimeInMillis());
       }
       return formatted;
    }
    	 public static Date getEmptyDate(Calendar dates) throws ParseException {
	 Date formatted=null;
		if (dates == null) {
			formatted = null;
		} else {

		//	SimpleDateFormat format1 = new SimpleDateFormat("dd-MM-yyyy");
			// System.out.println(CreatedDate.getTime());
			// Output "Wed Sep 26 14:23:28 EST 2012"
            formatted=new Date(dates.getTimeInMillis());
			//formatted = format1.parse(dates.getTimeInMillis());
		}

		return formatted;

	}
}
