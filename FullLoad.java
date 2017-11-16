import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.Properties;
import com.sforce.soap.enterprise.Connector;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.QueryResult;
import com.sforce.soap.enterprise.sobject.APTS_Product_Forecast__c;
import com.sforce.soap.enterprise.sobject.Apttus__APTS_Agreement__c;
import com.sforce.soap.enterprise.sobject.OpportunityHistory;
import com.sforce.soap.enterprise.sobject.Project__c;
import com.sforce.soap.enterprise.sobject.RecordType;
import com.sforce.soap.enterprise.sobject.SYN_Opportunity_Pipeline_Forecast__c;
import com.sforce.soap.enterprise.sobject.SYN_Price_Table__c;
import com.sforce.soap.enterprise.sobject.Sales_Transaction__c;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

/*
 * Here we fetch the whole data from the required table from source(Salesforce db) using
 * enterprise wsdl and insert it into the destination(Synaptics db).
 * Exception: For table Account, Product2, Opportunity data is fetched from patner wsdl and inserted
 * into temporary tables and from these temporary tables, the data is inserted into the required
 * tables.
 * Run PartnerWSDL before running FullLoad.
 * Pass vm arguments “-Dhttps.protocols=TLSv1.1,TLSv1.2” while running the script.
 * The execution time depends upon amount of data present in each table and the network speed.
 * Comments are provided at necessary places only at the first occurrence of that situation.
 * General methods description:
 * 1) query*: These methods first truncate the respective tables and fetch the data from
 * Salesforce db and store it in the code.
 * 2) insert*: These methods insert the fetched data in query* to Synaptics db.
 * 3) handle*: These methods convert the datatypes into a format acceptable in Synaptics db. Also,
 * if a datatype is null we have to manually assign null or 0 depending on the situation.
 */
final class FullLoad {
  private static String usernameSource;
  private static String usernameDestination;
  private static String passwordSource;
  private static String passwordDestination;
  private static String lastRunnedTime;
  private static EnterpriseConnection sourceConnection;

  public static void main(String[] args) {
    getPropertiesFile();
    ConnectorConfig config = new ConnectorConfig();
    config.setUsername(usernameSource);
    config.setPassword(passwordSource);
    config.setAuthEndpoint("https://synaptics.my.salesforce.com/services/Soap/c/40.0");
    try {
      sourceConnection = Connector.newConnection(config);
      /*
       * Used for testing. Don't remove.
       */
      // querySYNPriceTable__c();
      // querySalesTransaction__c();
      // queryProject__c();
      // queryApttusAPTSAgreement__c();
      // queryAccount();
      // queryProduct2();
      // queryOpportunity();
      // queryRecordType();
      // querySynOppoFore();
      // queryAPTSProForecast();
      // queryOppoHistory();
      Thread t1 = new Thread(new Runnable() {
        public void run() {
          querySYNPriceTable__c();
        }
      });
      Thread t2 = new Thread(new Runnable() {
        public void run() {
          querySalesTransaction__c();
        }
      });
      Thread t3 = new Thread(new Runnable() {
        public void run() {
          queryProject__c();
        }
      });
      Thread t4 = new Thread(new Runnable() {
        public void run() {
          queryApttusAPTSAgreement__c();
        }
      });
      Thread t5 = new Thread(new Runnable() {
        public void run() {
          queryAccount();
        }
      });
      Thread t6 = new Thread(new Runnable() {
        public void run() {
          queryProduct2();
        }
      });
      Thread t7 = new Thread(new Runnable() {
        public void run() {
          queryOpportunity();
        }
      });
      Thread t8 = new Thread(new Runnable() {
        public void run() {
          queryRecordType();
        }
      });
      Thread t9 = new Thread(new Runnable() {
        public void run() {
          querySynOppoFore();
        }
      });
      Thread t10 = new Thread(new Runnable() {
        public void run() {
          queryAPTSProForecast();
        }
      });
      Thread t11 = new Thread(new Runnable() {
        public void run() {
          queryOppoHistory();
        }
      });
      t1.start();
      t2.start();
      t3.start();
      t4.start();
      t5.start();
      t6.start();
      t7.start();
      t8.start();
      t9.start();
      t10.start();
      t11.start();
      t1.join();
      t2.join();
      t3.join();
      t4.join();
      t5.join();
      t6.join();
      t7.join();
      t8.join();
      t9.join();
      t10.join();
      t11.join();
      System.out.println("Process completed");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (sourceConnection != null) {
        try {
          sourceConnection.logout();
        } catch (ConnectionException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void querySYNPriceTable__c() {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1.prepareStatement("Truncate Table SYN_Price_Table__c");
      preparedStatement1.executeUpdate();
      QueryResult queryResults = sourceConnection.query("Select Id, OwnerId, IsDeleted, Name, "
          + "CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, "
          + "SystemModstamp, LastViewedDate, LastReferencedDate,"
          + " Bill_to_Region__c, Bill_to_customer__c, End_Application__c, "
          + "External_ID__c, Incentive__c, Non_Standard_Price_Agreement__c, "
          + "OEM_Rebate_Agreement__c, OEM_Rebate_Price_Rule_Entry__c, "
          + "Opportunity_Agreement__c, Opportunity__c, Part_Number__c, "
          + "Price_Agreement_Account__c, Price_Agreement_Id__c, Price_Agreement_Name__c, "
          + "Price_Effective_Date__c, Price_Rule_Entry__c, Price_Rule__c, Product_Series__c, "
          + "Row_Number_Internal_Use__c, Supply_Chain_ID__c, "
          + "Tier1_Agreement_Owner__c, Buy_Sell__c, Effective_Date__c, "
          + "Expiration_Date__c, From_Quantity__c, Gross_Price__c, "
          + "Is_Price_Agreement_Valid__c, MS_Code__c, "
          + "Net_Net_Price__c, Net_Price_to_Synaptics__c, Net_Price_to_Tier1_Account__c, "
          + "OEM_Account__c, OEM_Gross_Price__c, OEM_Rebate_Amount__c, "
          + "OEM_Rebate_Party__c, OEM_Rebate_Type__c, "
          + "OEM_Rebate_Value__c, PO_Booking_Price__c, PO_Specific_Volume_Tiers__c,"
          + " Planning_Policy__c, Step_Pricing__c, Tier1_Account__c, Tier1_Rebate_Amount__c, "
          + "Tier1_Rebate_Party__c, "
          + "Tier1_Rebate_Type__c, Tier1_Rebate_Value__c, To_Quantity__c, "
          + "VAR_Disti_Margin__c, VAR_Disti_Rebate_Value__c, "
          + "VAR_Rebate_Party__c, Status__c,OEM_Account_Id__c,"
          + "Tier1_Account_Id__c,OEM_Account2__c, Tier1_Account2__c FROM SYN_Price_Table__c");
      boolean done = false;
      if (queryResults.getSize() > 0) {
        while (!done) {
          for (int i = 0; i < queryResults.getRecords().length; i++) {
            SYN_Price_Table__c c = (SYN_Price_Table__c) queryResults.getRecords()[i];
            String Id = c.getId();
            String OwnerId = c.getOwnerId();
            Boolean IsDeleted = c.getIsDeleted();
            String Name = c.getName();
            Timestamp CreatedDate = handleTimestamp(c.getCreatedDate());
            String CreatedById = c.getCreatedById();
            Timestamp LastModifiedDate = handleTimestamp(c.getLastModifiedDate());
            String LastModifiedById = c.getLastModifiedById();
            Timestamp SystemModstamp = handleTimestamp(c.getSystemModstamp());
            Timestamp LastViewedDate = handleTimestamp(c.getLastViewedDate());
            Timestamp LastReferencedDate = handleTimestamp(c.getLastReferencedDate());
            String Bill_to_Region__c = c.getBill_to_Region__c();
            String Bill_to_customer__c = c.getBill_to_customer__c();
            String End_Application__c = c.getEnd_Application__c();
            String External_ID__c = c.getExternal_ID__c();
            String Incentive__c = c.getIncentive__c();
            Boolean Non_Standard_Price_Agreement__c = c.getNon_Standard_Price_Agreement__c();
            String OEM_Rebate_Agreement__c = c.getOEM_Rebate_Agreement__c();
            String OEM_Rebate_Price_Rule_Entry__c = c.getOEM_Rebate_Price_Rule_Entry__c();
            String Opportunity_Agreement__c = c.getOpportunity_Agreement__c();
            String Opportunity__c = c.getOpportunity__c();
            String Part_Number__c = c.getPart_Number__c();
            String Price_Agreement_Account__c = c.getPrice_Agreement_Account__c();
            String Price_Agreement_Id__c = c.getPrice_Agreement_Id__c();
            String Price_Agreement_Name__c = c.getPrice_Agreement_Name__c();
            Date Price_Effective_Date__c = handleDate(c.getPrice_Effective_Date__c());
            String Price_Rule_Entry__c = c.getPrice_Rule_Entry__c();
            String Price_Rule__c = c.getPrice_Rule__c();
            String Product_Series__c = c.getProduct_Series__c();
            String Row_Number_Internal_Use__c = c.getRow_Number_Internal_Use__c();
            String Supply_Chain_ID__c = c.getSupply_Chain_ID__c();
            String Tier1_Agreement_Owner__c = c.getTier1_Agreement_Owner__c();
            Boolean Buy_Sell__c = c.getBuy_Sell__c();
            Date Effective_Date__c = handleDate(c.getEffective_Date__c());
            Date Expiration_Date__c = handleDate(c.getExpiration_Date__c());
            Double From_Quantity__c_1 = c.getFrom_Quantity__c();
            Double From_Quantity__c = handleDouble(From_Quantity__c_1);
            Double Gross_Price__c_1 = c.getGross_Price__c();
            Double Gross_Price__c = handleDouble(Gross_Price__c_1);
            Boolean Is_Price_Agreement_Valid__c = c.getIs_Price_Agreement_Valid__c();
            String MS_Code__c = c.getMS_Code__c();
            Double Net_Net_Price__c_1 = c.getNet_Net_Price__c();
            Double Net_Net_Price__c = handleDouble(Net_Net_Price__c_1);
            Double Net_Price_to_Synaptics__c_1 = c.getNet_Price_to_Synaptics__c();
            Double Net_Price_to_Synaptics__c = handleDouble(Net_Price_to_Synaptics__c_1);
            Double Net_Price_to_Tier1_Account__c_1 = c.getNet_Price_to_Tier1_Account__c();
            Double Net_Price_to_Tier1_Account__c = handleDouble(Net_Price_to_Tier1_Account__c_1);
            String OEM_Account__c = c.getOEM_Account__c();
            Double OEM_Gross_Price__c_1 = c.getOEM_Gross_Price__c();
            Double OEM_Gross_Price__c = handleDouble(OEM_Gross_Price__c_1);
            Double OEM_Rebate_Amount__c_1 = c.getOEM_Rebate_Amount__c();
            Double OEM_Rebate_Amount__c = handleDouble(OEM_Rebate_Amount__c_1);
            String OEM_Rebate_Party__c = c.getOEM_Rebate_Party__c();
            String OEM_Rebate_Type__c = c.getOEM_Rebate_Type__c();
            Double OEM_Rebate_Value__c_1 = c.getOEM_Rebate_Value__c();
            Double OEM_Rebate_Value__c = handleDouble(OEM_Rebate_Value__c_1);
            Double PO_Booking_Price__c_1 = c.getPO_Booking_Price__c();
            Double PO_Booking_Price__c = handleDouble(PO_Booking_Price__c_1);
            Boolean PO_Specific_Volume_Tiers__c = c.getPO_Specific_Volume_Tiers__c();
            String Planning_Policy__c = c.getPlanning_Policy__c();
            Boolean Step_Pricing__c = c.getStep_Pricing__c();
            String Tier1_Account__c = c.getTier1_Account__c();
            Double Tier1_Rebate_Amount__c_1 = c.getTier1_Rebate_Amount__c();
            Double Tier1_Rebate_Amount__c = handleDouble(Tier1_Rebate_Amount__c_1);
            String Tier1_Rebate_Party__c = c.getTier1_Rebate_Party__c();
            String Tier1_Rebate_Type__c = c.getTier1_Rebate_Type__c();
            Double Tier1_Rebate_Value__c_1 = c.getTier1_Rebate_Value__c();
            Double Tier1_Rebate_Value__c = handleDouble(Tier1_Rebate_Value__c_1);
            Double To_Quantity__c_1 = c.getTo_Quantity__c();
            Double To_Quantity__c = handleDouble(To_Quantity__c_1);
            Double VAR_Disti_Margin__c_1 = c.getVAR_Disti_Margin__c();
            Double VAR_Disti_Margin__c = handleDouble(VAR_Disti_Margin__c_1);
            Double VAR_Disti_Rebate_Value__c_1 = c.getVAR_Disti_Rebate_Value__c();
            Double VAR_Disti_Rebate_Value__c = handleDouble(VAR_Disti_Rebate_Value__c_1);
            String VAR_Rebate_Party__c = c.getVAR_Rebate_Party__c();
            String Status__c = c.getStatus__c();
            String OEM_Account_Id__c = c.getOEM_Account_Id__c();
            String Tier1_Account_Id__c = c.getTier1_Account_Id__c();
            String OEM_Account2__c = c.getOEM_Account2__c();
            String Tier1_Account2__c = c.getTier1_Account2__c();
            String ID2 = Id.substring(0, 15);
            insertSYNPriceTable__c(OwnerId, IsDeleted, Name, CreatedDate, CreatedById,
                LastModifiedDate, LastModifiedById, SystemModstamp, LastViewedDate,
                LastReferencedDate, Bill_to_Region__c, Bill_to_customer__c, End_Application__c,
                External_ID__c, Incentive__c, Non_Standard_Price_Agreement__c,
                OEM_Rebate_Agreement__c, OEM_Rebate_Price_Rule_Entry__c, Opportunity_Agreement__c,
                Opportunity__c, Part_Number__c, Price_Agreement_Account__c, Price_Agreement_Id__c,
                Price_Agreement_Name__c, Price_Effective_Date__c, Price_Rule_Entry__c,
                Price_Rule__c, Product_Series__c, Row_Number_Internal_Use__c, Supply_Chain_ID__c,
                Tier1_Agreement_Owner__c, Buy_Sell__c, Effective_Date__c, Expiration_Date__c,
                From_Quantity__c, Gross_Price__c, Is_Price_Agreement_Valid__c, MS_Code__c,
                Net_Net_Price__c, Net_Price_to_Synaptics__c, Net_Price_to_Tier1_Account__c,
                OEM_Account__c, OEM_Gross_Price__c, OEM_Rebate_Amount__c, OEM_Rebate_Party__c,
                OEM_Rebate_Type__c, OEM_Rebate_Value__c, PO_Booking_Price__c,
                PO_Specific_Volume_Tiers__c, Planning_Policy__c, Step_Pricing__c, Tier1_Account__c,
                Tier1_Rebate_Amount__c, Tier1_Rebate_Party__c, Tier1_Rebate_Type__c,
                Tier1_Rebate_Value__c, To_Quantity__c, VAR_Disti_Margin__c,
                VAR_Disti_Rebate_Value__c, VAR_Rebate_Party__c, Status__c, OEM_Account_Id__c,
                Tier1_Account_Id__c, Id, ID2, OEM_Account2__c, Tier1_Account2__c);
            System.out.println("Inserting into SYNPriceTable__c Id: " + Id);
          }
          if (queryResults.isDone()) {
            done = true;
          } else {
            /*
             * Use this call to process query() calls that retrieve a large number of records (by
             * default, more than 500) in the result set. The query() call retrieves the first 500
             * records and creates a server-side cursor that is represented in the queryLocator
             * object. The queryMore() call processes subsequent records in up to 500-record chunks,
             * resets the server-side cursor, and returns a newly generated QueryLocator.
             */
            queryResults = sourceConnection.queryMore(queryResults.getQueryLocator());
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void insertSYNPriceTable__c(String OwnerId, Boolean IsDeleted, String Name,
      Timestamp CreatedDate, String CreatedById, Timestamp LastModifiedDate,
      String LastModifiedById, Timestamp SystemModstamp, Timestamp LastViewedDate,
      Timestamp LastReferencedDate, String Bill_to_Region__c, String Bill_to_customer__c,
      String End_Application__c, String External_ID__c, String Incentive__c,
      Boolean Non_Standard_Price_Agreement__c, String OEM_Rebate_Agreement__c,
      String OEM_Rebate_Price_Rule_Entry__c, String Opportunity_Agreement__c, String Opportunity__c,
      String Part_Number__c, String Price_Agreement_Account__c, String Price_Agreement_Id__c,
      String Price_Agreement_Name__c, Date Price_Effective_Date__c, String Price_Rule_Entry__c,
      String Price_Rule__c, String Product_Series__c, String Row_Number_Internal_Use__c,
      String Supply_Chain_ID__c, String Tier1_Agreement_Owner__c, Boolean Buy_Sell__c,
      Date Effective_Date__c, Date Expiration_Date__c, Double From_Quantity__c,
      Double Gross_Price__c, Boolean Is_Price_Agreement_Valid__c, String MS_Code__c,
      Double Net_Net_Price__c, Double Net_Price_to_Synaptics__c,
      Double Net_Price_to_Tier1_Account__c, String OEM_Account__c, Double OEM_Gross_Price__c,
      Double OEM_Rebate_Amount__c, String OEM_Rebate_Party__c, String OEM_Rebate_Type__c,
      Double OEM_Rebate_Value__c, Double PO_Booking_Price__c, Boolean PO_Specific_Volume_Tiers__c,
      String Planning_Policy__c, Boolean Step_Pricing__c, String Tier1_Account__c,
      Double Tier1_Rebate_Amount__c, String Tier1_Rebate_Party__c, String Tier1_Rebate_Type__c,
      Double Tier1_Rebate_Value__c, Double To_Quantity__c, Double VAR_Disti_Margin__c,
      Double VAR_Disti_Rebate_Value__c, String VAR_Rebate_Party__c, String Status__c,
      String OEM_Account_Id__c, String Tier1_Account_Id__c, String Id, String ID2,
      String OEM_Account2__c, String Tier1_Account2__c) {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 =
          connection1.prepareStatement("INSERT INTO SYN_PRICE_TABLE__C(OWNERID, ISDELETED, NAME, "
              + "CREATEDDATE, CREATEDBYID, LASTMODIFIEDDATE, "
              + "LASTMODIFIEDBYID, SYSTEMMODSTAMP, LASTVIEWEDDATE, "
              + "LASTREFERENCEDDATE, BILL_TO_REGION__C, BILL_TO_CUSTOMER__C, "
              + "END_APPLICATION__C, EXTERNAL_ID__C, INCENTIVE__C, "
              + "NON_STAND_PRICE_AGREEMENT__C, OEM_REBATE_AGREEMENT__C, "
              + "OEM_REBATE_PRICE_RULE_ENTRY__C, OPPORTUNITY_AGREEMENT__C, "
              + "OPPORTUNITY__C, PART_NUMBER__C, "
              + "PRICE_AGREEMENT_ACCOUNT__C, PRICE_AGREEMENT_ID__C, "
              + "PRICE_AGREEMENT_NAME__C, PRICE_EFFECTIVE_DATE__C, "
              + "PRICE_RULE_ENTRY__C, PRICE_RULE__C, PRODUCT_SERIES__C, "
              + "ROW_NUMBER_INTERNAL_USE__C, SUPPLY_CHAIN_ID__C, "
              + "TIER1_AGREEMENT_OWNER__C, BUY_SELL__C, EFFECTIVE_DATE__C, "
              + "EXPIRATION_DATE__C, FROM_QUANTITY__C, "
              + "GROSS_PRICE__C, IS_PRICE_AGREEMENT_VALID__C, MS_CODE__C, "
              + "NET_NET_PRICE__C, NET_PRICE_TO_SYNAPTICS__C, "
              + "NET_PRICE_TO_TIER1_ACCOUNT__C, OEM_ACCOUNT__C, OEM_GROSS_PRICE__C, "
              + "OEM_REBATE_AMOUNT__C, OEM_REBATE_PARTY__C, "
              + "OEM_REBATE_TYPE__C, OEM_REBATE_VALUE__C, PO_BOOKING_PRICE__C, "
              + "PO_SPECIFIC_VOLUME_TIERS__C, PLANNING_POLICY__C, "
              + "STEP_PRICING__C, TIER1_ACCOUNT__C, TIER1_REBATE_AMOUNT__C, "
              + "TIER1_REBATE_PARTY__C, TIER1_REBATE_TYPE__C, "
              + "TIER1_REBATE_VALUE__C, TO_QUANTITY__C, VAR_DISTI_MARGIN__C, "
              + "VAR_DISTI_REBATE_VALUE__C, VAR_REBATE_PARTY__C, STATUS__C, OEM_ACCOUNT_ID__C, "
              + "TIER1_ACCOUNT_ID__C, ID, ID2, OEM_ACCOUNT2__C, TIER1_ACCOUNT2__C)"
              + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      preparedStatement1.setString(1, OwnerId);
      preparedStatement1.setBoolean(2, IsDeleted);
      preparedStatement1.setString(3, Name);
      preparedStatement1.setTimestamp(4, CreatedDate);
      preparedStatement1.setString(5, CreatedById);
      preparedStatement1.setTimestamp(6, LastModifiedDate);
      preparedStatement1.setString(7, LastModifiedById);
      preparedStatement1.setTimestamp(8, SystemModstamp);
      preparedStatement1.setTimestamp(9, LastViewedDate);
      preparedStatement1.setTimestamp(10, LastReferencedDate);
      preparedStatement1.setString(11, Bill_to_Region__c);
      preparedStatement1.setString(12, Bill_to_customer__c);
      preparedStatement1.setString(13, End_Application__c);
      preparedStatement1.setString(14, External_ID__c);
      preparedStatement1.setString(15, Incentive__c);
      preparedStatement1.setBoolean(16, Non_Standard_Price_Agreement__c);
      preparedStatement1.setString(17, OEM_Rebate_Agreement__c);
      preparedStatement1.setString(18, OEM_Rebate_Price_Rule_Entry__c);
      preparedStatement1.setString(19, Opportunity_Agreement__c);
      preparedStatement1.setString(20, Opportunity__c);
      preparedStatement1.setString(21, Part_Number__c);
      preparedStatement1.setString(22, Price_Agreement_Account__c);
      preparedStatement1.setString(23, Price_Agreement_Id__c);
      preparedStatement1.setString(24, Price_Agreement_Name__c);
      preparedStatement1.setDate(25, Price_Effective_Date__c);
      preparedStatement1.setString(26, Price_Rule_Entry__c);
      preparedStatement1.setString(27, Price_Rule__c);
      preparedStatement1.setString(28, Product_Series__c);
      preparedStatement1.setString(29, Row_Number_Internal_Use__c);
      preparedStatement1.setString(30, Supply_Chain_ID__c);
      preparedStatement1.setString(31, Tier1_Agreement_Owner__c);
      preparedStatement1.setBoolean(32, Buy_Sell__c);
      preparedStatement1.setDate(33, Effective_Date__c);
      preparedStatement1.setDate(34, Expiration_Date__c);
      preparedStatement1.setDouble(35, From_Quantity__c);
      preparedStatement1.setDouble(36, Gross_Price__c);
      preparedStatement1.setBoolean(37, Is_Price_Agreement_Valid__c);
      preparedStatement1.setString(38, MS_Code__c);
      preparedStatement1.setDouble(39, Net_Net_Price__c);
      preparedStatement1.setDouble(40, Net_Price_to_Synaptics__c);
      preparedStatement1.setDouble(41, Net_Price_to_Tier1_Account__c);
      preparedStatement1.setString(42, OEM_Account__c);
      preparedStatement1.setDouble(43, OEM_Gross_Price__c);
      preparedStatement1.setDouble(44, OEM_Rebate_Amount__c);
      preparedStatement1.setString(45, OEM_Rebate_Party__c);
      preparedStatement1.setString(46, OEM_Rebate_Type__c);
      preparedStatement1.setDouble(47, OEM_Rebate_Value__c);
      preparedStatement1.setDouble(48, PO_Booking_Price__c);
      preparedStatement1.setBoolean(49, PO_Specific_Volume_Tiers__c);
      preparedStatement1.setString(50, Planning_Policy__c);
      preparedStatement1.setBoolean(51, Step_Pricing__c);
      preparedStatement1.setString(52, Tier1_Account__c);
      preparedStatement1.setDouble(53, Tier1_Rebate_Amount__c);
      preparedStatement1.setString(54, Tier1_Rebate_Party__c);
      preparedStatement1.setString(55, Tier1_Rebate_Type__c);
      preparedStatement1.setDouble(56, Tier1_Rebate_Value__c);
      preparedStatement1.setDouble(57, To_Quantity__c);
      preparedStatement1.setDouble(58, VAR_Disti_Margin__c);
      preparedStatement1.setDouble(59, VAR_Disti_Rebate_Value__c);
      preparedStatement1.setString(60, VAR_Rebate_Party__c);
      preparedStatement1.setString(61, Status__c);
      preparedStatement1.setString(62, OEM_Account_Id__c);
      preparedStatement1.setString(63, Tier1_Account_Id__c);
      preparedStatement1.setString(64, Id);
      preparedStatement1.setString(65, ID2);
      preparedStatement1.setString(66, OEM_Account2__c);
      preparedStatement1.setString(67, Tier1_Account2__c);
      preparedStatement1.executeUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void querySalesTransaction__c() {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1.prepareStatement("Truncate Table Sales_Transaction__c");
      preparedStatement1.executeUpdate();
      QueryResult queryResults = sourceConnection
          .query("Select Id, OwnerId, IsDeleted, Name, CreatedDate, CreatedById, LastModifiedDate, "
              + "LastModifiedById, SystemModstamp, Actual_Shipment_Date__c, Agreement_Account__c, "
              + "Approval_Status__c, Backlog_Or_Billing__c, Bill_To__c, Cancel_Reason_Code__c, "
              + "Cust_PO_Number__c, Cust_Part_Number__c, Data_Type__c, Gross_Amount__c, "
              + "Gross_Price__c, Invoice_Date__c, Line_Number__c, Must_Ship_Date__c, "
              + "Net_Amount__c, OEM__c, OPA_ID__c, Opportunity_ID__c, Opportunity__c, "
              + "Order_Approver__c, Order_Category__c,Order_Date__c, Order_Line_ID__c, "
              + "Order_Number__c, Order_Status__c, Part_Number__c, Product_Series__c, "
              + "Quantity__c, Rebate_Amount__c, "
              + "Request_Date__c, Revenue_Or_Sample__c, Schedule_Ship_Date__c, Tier1_Account__c, "
              + "Tier1_Data_Type__c, Transaction_Date__c, MS_Code__c FROM Sales_Transaction__c");
      boolean done = false;
      if (queryResults.getSize() > 0) {
        while (!done) {
          for (int i = 0; i < queryResults.getRecords().length; i++) {
            Sales_Transaction__c c = (Sales_Transaction__c) queryResults.getRecords()[i];
            String Id = c.getId();
            String OwnerId = c.getOwnerId();
            Boolean IsDeleted = c.getIsDeleted();
            String Name = c.getName();
            Calendar CreatedDate_1 = c.getCreatedDate();
            Date CreatedDate = handleDate(CreatedDate_1);
            String CreatedById = c.getCreatedById();
            Calendar LastModifiedDate_1 = c.getLastModifiedDate();
            Timestamp LastModifiedDate = handleTimestamp(LastModifiedDate_1);
            String LastModifiedById = c.getLastModifiedById();
            Calendar SystemModstamp_1 = c.getSystemModstamp();
            Timestamp SystemModstamp = handleTimestamp(SystemModstamp_1);
            Calendar Actual_Shipment_Date__c_1 = c.getActual_Shipment_Date__c();
            Date Actual_Shipment_Date__c = handleDate(Actual_Shipment_Date__c_1);
            String Agreement_Account__c = c.getAgreement_Account__c();
            String Approval_Status__c = c.getApproval_Status__c();
            String Backlog_Or_Billing__c = c.getBacklog_Or_Billing__c();
            String Bill_To__c = c.getBill_To__c();
            String Cancel_Reason_Code__c = c.getCancel_Reason_Code__c();
            String Cust_PO_Number__c = c.getCust_PO_Number__c();
            String Cust_Part_Number__c = c.getCust_Part_Number__c();
            String Data_Type__c = c.getData_Type__c();
            Double Gross_Amount__c_1 = c.getGross_Amount__c();
            Double Gross_Amount__c = handleDouble(Gross_Amount__c_1);
            Double Gross_Price__c_1 = c.getGross_Price__c();
            Double Gross_Price__c = handleDouble(Gross_Price__c_1);
            Calendar Invoice_Date__c_1 = c.getInvoice_Date__c();
            Date Invoice_Date__c = handleDate(Invoice_Date__c_1);
            String Line_Number__c = c.getLine_Number__c();
            Calendar Must_Ship_Date__c_1 = c.getMust_Ship_Date__c();
            Date Must_Ship_Date__c = handleDate(Must_Ship_Date__c_1);
            Double Net_Amount__c_1 = c.getNet_Amount__c();
            Double Net_Amount__c = handleDouble(Net_Amount__c_1);
            String OEM__c = c.getOEM__c();
            String OPA_ID__c = c.getOPA_ID__c();
            String Opportunity_ID__c = c.getOpportunity_ID__c();
            String Opportunity__c = c.getOpportunity__c();
            String Order_Approver__c = c.getOrder_Approver__c();
            String Order_Category__c = c.getOrder_Category__c();
            Calendar Order_Date__c_1 = c.getOrder_Date__c();
            Date Order_Date__c = handleDate(Order_Date__c_1);
            Double Order_Line_ID__c_1 = c.getOrder_Line_ID__c();
            Double Order_Line_ID__c = handleDouble(Order_Line_ID__c_1);
            String Order_Number__c = c.getOrder_Number__c();
            String Order_Status__c = c.getOrder_Status__c();
            String Part_Number__c = c.getPart_Number__c();
            String Product_Series__c = c.getProduct_Series__c();
            Double Quantity__c_1 = c.getQuantity__c();
            Double Quantity__c = handleDouble(Quantity__c_1);
            Double Rebate_Amount__c_1 = c.getRebate_Amount__c();
            Double Rebate_Amount__c = handleDouble(Rebate_Amount__c_1);
            Calendar Request_Date__c_1 = c.getRequest_Date__c();
            Date Request_Date__c = handleDate(Request_Date__c_1);
            String Revenue_Or_Sample__c = c.getRevenue_Or_Sample__c();
            Calendar Schedule_Ship_Date__c_1 = c.getSchedule_Ship_Date__c();
            Date Schedule_Ship_Date__c = handleDate(Schedule_Ship_Date__c_1);
            String Tier1_Account__c = c.getTier1_Account__c();
            String Tier1_Data_Type__c = c.getTier1_Data_Type__c();
            Calendar Transaction_Date__c_1 = c.getTransaction_Date__c();
            Date Transaction_Date__c = handleDate(Transaction_Date__c_1);
            String MS_Code__c = c.getMS_Code__c();
            String ID2 = Id.substring(0, 15);
            insertSalesTransaction__c(/* Id, */OwnerId, IsDeleted, Name, CreatedDate, CreatedById,
                LastModifiedDate, LastModifiedById, SystemModstamp, Actual_Shipment_Date__c,
                Agreement_Account__c, Approval_Status__c, Backlog_Or_Billing__c, Bill_To__c,
                Cancel_Reason_Code__c, Cust_PO_Number__c, Cust_Part_Number__c, Data_Type__c,
                Gross_Amount__c, Gross_Price__c, Invoice_Date__c, Line_Number__c, Must_Ship_Date__c,
                Net_Amount__c, OEM__c, OPA_ID__c, Opportunity_ID__c, Opportunity__c,
                Order_Approver__c, Order_Category__c, Order_Date__c, Order_Line_ID__c,
                Order_Number__c, Order_Status__c, Part_Number__c, Product_Series__c, Quantity__c,
                Rebate_Amount__c, Request_Date__c, Revenue_Or_Sample__c, Schedule_Ship_Date__c,
                Tier1_Account__c, Tier1_Data_Type__c, Transaction_Date__c, MS_Code__c, Id, ID2);
            System.out.println("Inserting into SalesTransaction Id: " + Id);
          }
          if (queryResults.isDone()) {
            done = true;
          } else {
            queryResults = sourceConnection.queryMore(queryResults.getQueryLocator());
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void insertSalesTransaction__c(String OwnerId, Boolean IsDeleted, String Name,
      Date CreatedDate, String CreatedById, Timestamp LastModifiedDate, String LastModifiedById,
      Timestamp SystemModstamp, Date Actual_Shipment_Date__c, String Agreement_Account__c,
      String Approval_Status__c, String Backlog_Or_Billing__c, String Bill_To__c,
      String Cancel_Reason_Code__c, String Cust_PO_Number__c, String Cust_Part_Number__c,
      String Data_Type__c, Double Gross_Amount__c, Double Gross_Price__c, Date Invoice_Date__c,
      String Line_Number__c, Date Must_Ship_Date__c, Double Net_Amount__c, String OEM__c,
      String OPA_ID__c, String Opportunity_ID__c, String Opportunity__c, String Order_Approver__c,
      String Order_Category__c, Date Order_Date__c, Double Order_Line_ID__c, String Order_Number__c,
      String Order_Status__c, String Part_Number__c, String Product_Series__c, Double Quantity__c,
      Double Rebate_Amount__c, Date Request_Date__c, String Revenue_Or_Sample__c,
      Date Schedule_Ship_Date__c, String Tier1_Account__c, String Tier1_Data_Type__c,
      Date Transaction_Date__c, String MS_Code__c, String Id, String ID2) {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1.prepareStatement(
          "INSERT INTO Sales_Transaction__c(OWNERID, ISDELETED, NAME, CREATEDDATE, CREATEDBYID, "
              + "LASTMODIFIEDDATE, LASTMODIFIEDBYID, SYSTEMMODSTAMP, "
              + "ACTUAL_SHIPMENT_DATE__C, AGREEMENT_ACCOUNT__C, APPROVAL_STATUS__C, "
              + "BACKLOG_OR_BILLING__C, BILL_TO__C, CANCEL_REASON_CODE__C, "
              + "CUST_PO_NUMBER__C, CUST_PART_NUM__C, DATA_TYPE__C, GROSS_AMOUNT__C, "
              + "GROSS_PRICE__C, INVOICE_DATE__C, LINE_NUMBER__C, MUST_SHIP_DATE__C, "
              + "NET_AMOUNT__C, OEM__C, OPA_ID__C, OPPORTUNITY_ID__C, OPPORTUNITY__C, "
              + "ORDER_APPROVER__C, ORDER_CATEGORY__C, ORDER_DATE__C, ORDER_LINE_ID__C, "
              + "ORDER_NUMBER__C, ORDER_STATUS__C, PART_NUMBER__C, PRODUCT_SERIES__C, "
              + "QUANTITY__C, " + "REBATE_AMOUNT__C, REQUEST_DATE__C, REVENUE_OR_SAMPLE__C, "
              + "SCHEDULE_SHIP_DATE__C, TIER1_ACCOUNT__C, TIER1_DATA_TYPE__C, "
              + "TRANSACTION_DATE__C, " + "MS_CODE__C, ID, ID2)"
              + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      preparedStatement1.setString(1, OwnerId);
      preparedStatement1.setBoolean(2, IsDeleted);
      preparedStatement1.setString(3, Name);
      preparedStatement1.setDate(4, CreatedDate);
      preparedStatement1.setString(5, CreatedById);
      preparedStatement1.setTimestamp(6, LastModifiedDate);
      preparedStatement1.setString(7, LastModifiedById);
      preparedStatement1.setTimestamp(8, SystemModstamp);
      preparedStatement1.setDate(9, Actual_Shipment_Date__c);
      preparedStatement1.setString(10, Agreement_Account__c);
      preparedStatement1.setString(11, Approval_Status__c);
      preparedStatement1.setString(12, Backlog_Or_Billing__c);
      preparedStatement1.setString(13, Bill_To__c);
      preparedStatement1.setString(14, Cancel_Reason_Code__c);
      preparedStatement1.setString(15, Cust_PO_Number__c);
      preparedStatement1.setString(16, Cust_Part_Number__c);
      preparedStatement1.setString(17, Data_Type__c);
      preparedStatement1.setDouble(18, Gross_Amount__c);
      preparedStatement1.setDouble(19, Gross_Price__c);
      preparedStatement1.setDate(20, Invoice_Date__c);
      preparedStatement1.setString(21, Line_Number__c);
      preparedStatement1.setDate(22, Must_Ship_Date__c);
      preparedStatement1.setDouble(23, Net_Amount__c);
      preparedStatement1.setString(24, OEM__c);
      preparedStatement1.setString(25, OPA_ID__c);
      preparedStatement1.setString(26, Opportunity_ID__c);
      preparedStatement1.setString(27, Opportunity__c);
      preparedStatement1.setString(28, Order_Approver__c);
      preparedStatement1.setString(29, Order_Category__c);
      preparedStatement1.setDate(30, Order_Date__c);
      preparedStatement1.setDouble(31, Order_Line_ID__c);
      preparedStatement1.setString(32, Order_Number__c);
      preparedStatement1.setString(33, Order_Status__c);
      preparedStatement1.setString(34, Part_Number__c);
      preparedStatement1.setString(35, Product_Series__c);
      preparedStatement1.setDouble(36, Quantity__c);
      preparedStatement1.setDouble(37, Rebate_Amount__c);
      preparedStatement1.setDate(38, Request_Date__c);
      preparedStatement1.setString(39, Revenue_Or_Sample__c);
      preparedStatement1.setDate(40, Schedule_Ship_Date__c);
      preparedStatement1.setString(41, Tier1_Account__c);
      preparedStatement1.setString(42, Tier1_Data_Type__c);
      preparedStatement1.setDate(43, Transaction_Date__c);
      preparedStatement1.setString(44, MS_Code__c);
      preparedStatement1.setString(45, Id);
      preparedStatement1.setString(46, ID2);
      preparedStatement1.executeUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void queryProject__c() {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1.prepareStatement("Truncate Table PROJECT_C");
      preparedStatement1.executeUpdate();
      QueryResult queryResults =
          sourceConnection.query("Select Id,OwnerId, IsDeleted, Name, RecordTypeId, "
              + "CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, "
              + "SystemModstamp, LastActivityDate,"
              + " LastViewedDate, LastReferencedDate, Account__c, "
              + "Allocated_OEM_Project_Forecast__c, Distributor__c,"
              + " ES_Date__c, Code_Name__c, Fab__c, Gen__c, Lifetime_Volume_SAM__c, "
              + "Customer_Contact__c, Panel_Border_Size__c,"
              + " Decision_Date__c, Panel_Resolution__c, Panel_Technology__c, "
              + "Panel_size_s__c, Primary_Solution_Series__c, "
              + "Qualification_Date__c, Secondary_Solution_Series__c, "
              + "Tier1_Project_Forecast__c, Project_Actual_Hours__c, "
              + "Project_Estimated_Hours__c,"
              + " Customer_MP_Date__c, Customer_Quantity_Project_Volume__c, Device_Name__c, "
              + "End_Application__c, Features__c, Industry__c, "
              + "Lifetime_Quantity__c, Model_Name_in_Market__c, Net_Revenue__c, Notes__c, "
              + "Number_of_Design_Wins__c, Number_of_Months_Lifetime__c, "
              + "Opportunity_Stage_Value_Rollup__c, Private_Project__c, Project_ID__c, "
              + "Standardized_Project_Name_Indexed__c, Purpose__c, Region__c, Related_Project__c,"
              + " Stage__c, End_Market_Segmentation__c, Flagship__c, Standard_Project_Stage__c, "
              + "Standardized_NRG_Project_Name__c, Standardized_Project_Name__c, "
              + "Project_Owner_Name__c, "
              + "Type__c, RFQ__c, OEM_1__c, OEM_2__c, Discrete_Touch__c, Small_Display__c, "
              + "Large_Display__c, Fingerprint__c, Touchpad__c, Other__c, "
              + "Automotive_OEM_Account__c, Multimedia__c FROM Project__c");
      boolean done = false;
      if (queryResults.getSize() > 0) {
        while (!done) {
          for (int i = 0; i < queryResults.getRecords().length; i++) {
            Project__c c = (Project__c) queryResults.getRecords()[i];
            String Id = c.getId();
            String OwnerId = c.getOwnerId();
            Boolean IsDeleted = c.getIsDeleted();
            String Name = c.getName();
            String RecordTypeId = c.getRecordTypeId();
            Calendar CreatedDate_1 = c.getCreatedDate();
            Date CreatedDate = handleDate(CreatedDate_1);
            String CreatedById = c.getCreatedById();
            Calendar LastModifiedDate_1 = c.getLastModifiedDate();
            Timestamp LastModifiedDate = handleTimestamp(LastModifiedDate_1);
            String LastModifiedById = c.getLastModifiedById();
            Calendar SystemModstamp_1 = c.getSystemModstamp();
            Timestamp SystemModstamp = handleTimestamp(SystemModstamp_1);
            Calendar LastActivityDate_1 = c.getLastActivityDate();
            Date LastActivityDate = handleDate(LastActivityDate_1);
            Calendar LastViewedDate_1 = c.getLastViewedDate();
            Timestamp LastViewedDate = handleTimestamp(LastViewedDate_1);
            Calendar LastReferencedDate_1 = c.getLastReferencedDate();
            Timestamp LastReferencedDate = handleTimestamp(LastReferencedDate_1);
            String Account__c = c.getAccount__c();
            Double Allocated_OEM_Project_Forecast__c_1 = c.getAllocated_OEM_Project_Forecast__c();
            Double Allocated_OEM_Project_Forecast__c =
                handleDouble(Allocated_OEM_Project_Forecast__c_1);
            String Distributor__c = c.getDistributor__c();
            Calendar ES_Date__c_1 = c.getES_Date__c();
            Date ES_Date__c = handleDate(ES_Date__c_1);
            String Code_Name__c = c.getCode_Name__c();
            String Fab__c = c.getFab__c();
            String Gen__c = c.getGen__c();
            Double Lifetime_Volume_SAM__c_1 = c.getLifetime_Volume_SAM__c();
            Double Lifetime_Volume_SAM__c = handleDouble(Lifetime_Volume_SAM__c_1);
            String Customer_Contact__c = c.getCustomer_Contact__c();
            Double Panel_Border_Size__c_1 = c.getPanel_Border_Size__c();
            Double Panel_Border_Size__c = handleDouble(Panel_Border_Size__c_1);
            Calendar Decision_Date__c_1 = c.getDecision_Date__c();
            Date Decision_Date__c = handleDate(Decision_Date__c_1);
            String Panel_Resolution__c = c.getPanel_Resolution__c();
            String Panel_Technology__c = c.getPanel_Technology__c();
            Double Panel_size_s__c_1 = c.getPanel_size_s__c();
            Double Panel_size_s__c = handleDouble(Panel_size_s__c_1);
            String Primary_Solution_Series__c = c.getPrimary_Solution_Series__c();
            Calendar Qualification_Date__c_1 = c.getQualification_Date__c();
            Date Qualification_Date__c = handleDate(Qualification_Date__c_1);
            String Secondary_Solution_Series__c = c.getSecondary_Solution_Series__c();
            Double Tier1_Project_Forecast__c_1 = c.getTier1_Project_Forecast__c();
            Double Tier1_Project_Forecast__c = handleDouble(Tier1_Project_Forecast__c_1);
            Double Project_Actual_Hours__c_1 = c.getProject_Actual_Hours__c();
            Double Project_Actual_Hours__c = handleDouble(Project_Actual_Hours__c_1);
            Double Project_Estimated_Hours__c_1 = c.getProject_Estimated_Hours__c();
            Double Project_Estimated_Hours__c = handleDouble(Project_Estimated_Hours__c_1);
            Calendar Customer_MP_Date__c_1 = c.getCustomer_MP_Date__c();
            Date Customer_MP_Date__c = handleDate(Customer_MP_Date__c_1);
            Double Customer_Quantity_Project_Volume__c_1 =
                c.getCustomer_Quantity_Project_Volume__c();
            Double Customer_Quantity_Project_Volume__c =
                handleDouble(Customer_Quantity_Project_Volume__c_1);
            String Device_Name__c = c.getDevice_Name__c();
            String End_Application__c = c.getEnd_Application__c();
            String Features__c = c.getFeatures__c();
            String Industry__c = c.getIndustry__c();
            Double Lifetime_Quantity__c_1 = c.getLifetime_Quantity__c();
            Double Lifetime_Quantity__c = handleDouble(Lifetime_Quantity__c_1);
            String Model_Name_in_Market__c = c.getModel_Name_in_Market__c();
            Double Net_Revenue__c_1 = c.getNet_Revenue__c();
            Double Net_Revenue__c = handleDouble(Net_Revenue__c_1);
            String Notes__c = c.getNotes__c();
            Double Number_of_Design_Wins__c_1 = c.getNumber_of_Design_Wins__c();
            Double Number_of_Design_Wins__c = handleDouble(Number_of_Design_Wins__c_1);
            Double Number_of_Months_Lifetime__c_1 = c.getNumber_of_Months_Lifetime__c();
            Double Number_of_Months_Lifetime__c = handleDouble(Number_of_Months_Lifetime__c_1);
            Double Opportunity_Stage_Value_Rollup__c_1 = c.getOpportunity_Stage_Value_Rollup__c();
            Double Opportunity_Stage_Value_Rollup__c =
                handleDouble(Opportunity_Stage_Value_Rollup__c_1);
            Boolean Private_Project__c = c.getPrivate_Project__c();
            String Project_ID__c = c.getProject_ID__c();
            String Standardized_Project_Name_Indexed__c =
                c.getStandardized_Project_Name_Indexed__c();
            String Purpose__c = c.getPurpose__c();
            String Region__c = c.getRegion__c();
            String Related_Project__c = c.getRelated_Project__c();
            String Stage__c = c.getStage__c();
            String End_Market_Segmentation__c = c.getEnd_Market_Segmentation__c();
            Boolean Flagship__c = c.getFlagship__c();
            String Standard_Project_Stage__c = c.getStandard_Project_Stage__c();
            String Standardized_NRG_Project_Name__c = c.getStandardized_NRG_Project_Name__c();
            String Standardized_Project_Name__c = c.getStandardized_Project_Name__c();
            String Project_Owner_Name__c = c.getProject_Owner_Name__c();
            String Type__c = c.getType__c();
            String RFQ__c = c.getRFQ__c();
            String OEM_1__c = c.getOEM_1__c();
            String OEM_2__c = c.getOEM_2__c();
            Boolean Discrete_Touch__c = c.getDiscrete_Touch__c();
            Boolean Small_Display__c = c.getSmall_Display__c();
            Boolean Large_Display__c = c.getLarge_Display__c();
            Boolean Fingerprint__c = c.getFingerprint__c();
            Boolean Touchpad__c = c.getTouchpad__c();
            Boolean Other__c = c.getOther__c();
            String Automotive_OEM_Account__c = c.getAutomotive_OEM_Account__c();
            Boolean Multimedia__c = c.getMultimedia__c();
            insertProject__c(Id, OwnerId, IsDeleted, Name, RecordTypeId, CreatedDate, CreatedById,
                LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate,
                LastViewedDate, LastReferencedDate, Account__c, Allocated_OEM_Project_Forecast__c,
                Distributor__c, ES_Date__c, Code_Name__c, Fab__c, Gen__c, Lifetime_Volume_SAM__c,
                Customer_Contact__c, Panel_Border_Size__c, Decision_Date__c, Panel_Resolution__c,
                Panel_Technology__c, Panel_size_s__c, Primary_Solution_Series__c,
                Qualification_Date__c, Secondary_Solution_Series__c, Tier1_Project_Forecast__c,
                Project_Actual_Hours__c, Project_Estimated_Hours__c, Customer_MP_Date__c,
                Customer_Quantity_Project_Volume__c, Device_Name__c, End_Application__c,
                Features__c, Industry__c, Lifetime_Quantity__c, Model_Name_in_Market__c,
                Net_Revenue__c, Notes__c, Number_of_Design_Wins__c, Number_of_Months_Lifetime__c,
                Opportunity_Stage_Value_Rollup__c, Private_Project__c, Project_ID__c,
                Standardized_Project_Name_Indexed__c, Purpose__c, Region__c, Related_Project__c,
                Stage__c, End_Market_Segmentation__c, Flagship__c, Standard_Project_Stage__c,
                Standardized_NRG_Project_Name__c, Standardized_Project_Name__c,
                Project_Owner_Name__c, Type__c, RFQ__c, OEM_1__c, OEM_2__c, Discrete_Touch__c,
                Small_Display__c, Large_Display__c, Fingerprint__c, Touchpad__c, Other__c,
                Automotive_OEM_Account__c, Multimedia__c);
            System.out.println("Inserting into Project__c Id: " + Id);
          }
          if (queryResults.isDone()) {
            done = true;
          } else {
            queryResults = sourceConnection.queryMore(queryResults.getQueryLocator());
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void insertProject__c(String Id, String OwnerId, Boolean IsDeleted, String Name,
      String RecordTypeId, Date CreatedDate, String CreatedById, Timestamp LastModifiedDate,
      String LastModifiedById, Timestamp SystemModstamp, Date LastActivityDate,
      Timestamp LastViewedDate, Timestamp LastReferencedDate, String Account__c,
      Double Allocated_OEM_Project_Forecast__c, String Distributor__c, Date ES_Date__c,
      String Code_Name__c, String Fab__c, String Gen__c, Double Lifetime_Volume_SAM__c,
      String Customer_Contact__c, Double Panel_Border_Size__c, Date Decision_Date__c,
      String Panel_Resolution__c, String Panel_Technology__c, Double Panel_size_s__c,
      String Primary_Solution_Series__c, Date Qualification_Date__c,
      String Secondary_Solution_Series__c, Double Tier1_Project_Forecast__c,
      Double Project_Actual_Hours__c, Double Project_Estimated_Hours__c, Date Customer_MP_Date__c,
      Double Customer_Quantity_Project_Volume__c, String Device_Name__c, String End_Application__c,
      String Features__c, String Industry__c, Double Lifetime_Quantity__c,
      String Model_Name_in_Market__c, Double Net_Revenue__c, String Notes__c,
      Double Number_of_Design_Wins__c, Double Number_of_Months_Lifetime__c,
      Double Opportunity_Stage_Value_Rollup__c, Boolean Private_Project__c, String Project_ID__c,
      String Standardized_Project_Name_Indexed__c, String Purpose__c, String Region__c,
      String Related_Project__c, String Stage__c, String End_Market_Segmentation__c,
      Boolean Flagship__c, String Standard_Project_Stage__c,
      String Standardized_NRG_Project_Name__c, String Standardized_Project_Name__c,
      String Project_Owner_Name__c, String Type__c, String RFQ__c, String OEM_1__c, String OEM_2__c,
      Boolean Discrete_Touch__c, Boolean Small_Display__c, Boolean Large_Display__c,
      Boolean Fingerprint__c, Boolean Touchpad__c, Boolean Other__c,
      String Automotive_OEM_Account__c, Boolean Multimedia__c) {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 =
          connection1.prepareStatement("INSERT INTO Project_c(ID, OWNERID, ISDELETED, NAME, "
              + "RECORDTYPEID, CREATEDDATE, CREATEDBYID, LASTMODIFIEDDATE, LASTMODIFIEDBYID, "
              + "SYSTEMMODSTAMP, LASTACTIVITYDATE, LASTVIEWEDDATE, LASTREFERENCEDDATE, ACCOUNT__C, "
              + "ALLOCATED_OEM_PROJ_FRST__C, DISTRIBUTOR__C, ES_DATE__C, CODE_NAME__C, FAB__C,"
              + " GEN__C, " + "LIFETIME_VOLUME_SAM__C, CUSTOMER_CONTACT__C, "
              + "PANEL_BORDER_SIZE__C, DECISION_DATE__C, PANEL_RESOLUTION__C, PANEL_TECHNOLOGY__C, "
              + "PANEL_SIZE_S__C, PRIMARY_SOLUTION_SERIES__C, "
              + "QUALIFICATION_DATE__C, SECONDARY_SOLUTION_SERIES__C, TIER1_PROJ_FORECAST__C, "
              + "PROJECT_ACTUAL_HOURS__C, PROJECT_ESTIMATED_HOURS__C, "
              + "CUSTOMER_MP_DATE__C, CUSTOMER_QTY_PROJ_VOL__C, DEVICE_NAME__C, "
              + "END_APPLICATION__C, FEATURES__C, INDUSTRY__C, "
              + "LIFETIME_QUANTITY__C, MODEL_NAME_IN_MARKET__C, NET_REVENUE__C, "
              + "NOTES__C, NUMBER_OF_DESIGN_WINS__C, NUMBER_OF_MONTHS_LIFETIME__C, "
              + "OPPORTUNITY_STG_VAL_ROLLUP__C, PRIVATE_PROJECT__C, PROJECT_ID__C, "
              + "STAND_PROJECT_NAME_INDEXED__C, PURPOSE__C, REGION__C, RELATED_PROJECT__C, "
              + "STAGE__C, END_MARKET_SEGMENTATION__C, FLAGSHIP__C, STANDARD_PROJECT_STAGE__C, "
              + "STAND_NRG_PROJ_NAME__C, STANDARDIZED_PROJECT_NAME__C, PROJECT_OWNER_NAME__C, "
              + "TYPE__C, RFQ__C, OEM_1__C, OEM_2__C, "
              + "DISCRETE_TOUCH__C, SMALL_DISPLAY__C, LARGE_DISPLAY__C, FINGERPRINT__C, "
              + "TOUCHPAD__C, OTHER__C, AUTOMOTIVE_OEM_ACCOUNT__C, MULTIMEDIA__C)"
              + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
              + " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + "?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
              + " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + "?, ?, ?, ?, ?, ?, ?, ?, ?)");
      preparedStatement1.setString(1, Id);
      preparedStatement1.setString(2, OwnerId);
      preparedStatement1.setBoolean(3, IsDeleted);
      preparedStatement1.setString(4, Name);
      preparedStatement1.setString(5, RecordTypeId);
      preparedStatement1.setDate(6, CreatedDate);
      preparedStatement1.setString(7, CreatedById);
      preparedStatement1.setTimestamp(8, LastModifiedDate);
      preparedStatement1.setString(9, LastModifiedById);
      preparedStatement1.setTimestamp(10, SystemModstamp);
      preparedStatement1.setDate(11, LastActivityDate);
      preparedStatement1.setTimestamp(12, LastViewedDate);
      preparedStatement1.setTimestamp(13, LastReferencedDate);
      preparedStatement1.setString(14, Account__c);
      preparedStatement1.setDouble(15, Allocated_OEM_Project_Forecast__c);
      preparedStatement1.setString(16, Distributor__c);
      preparedStatement1.setDate(17, ES_Date__c);
      preparedStatement1.setString(18, Code_Name__c);
      preparedStatement1.setString(19, Fab__c);
      preparedStatement1.setString(20, Gen__c);
      preparedStatement1.setDouble(21, Lifetime_Volume_SAM__c);
      preparedStatement1.setString(22, Customer_Contact__c);
      preparedStatement1.setDouble(23, Panel_Border_Size__c);
      preparedStatement1.setDate(24, Decision_Date__c);
      preparedStatement1.setString(25, Panel_Resolution__c);
      preparedStatement1.setString(26, Panel_Technology__c);
      preparedStatement1.setDouble(27, Panel_size_s__c);
      preparedStatement1.setString(28, Primary_Solution_Series__c);
      preparedStatement1.setDate(29, Qualification_Date__c);
      preparedStatement1.setString(30, Secondary_Solution_Series__c);
      preparedStatement1.setDouble(31, Tier1_Project_Forecast__c);
      preparedStatement1.setDouble(32, Project_Actual_Hours__c);
      preparedStatement1.setDouble(33, Project_Estimated_Hours__c);
      preparedStatement1.setDate(34, Customer_MP_Date__c);
      preparedStatement1.setDouble(35, Customer_Quantity_Project_Volume__c);
      preparedStatement1.setString(36, Device_Name__c);
      preparedStatement1.setString(37, End_Application__c);
      preparedStatement1.setString(38, Features__c);
      preparedStatement1.setString(39, Industry__c);
      preparedStatement1.setDouble(40, Lifetime_Quantity__c);
      preparedStatement1.setString(41, Model_Name_in_Market__c);
      preparedStatement1.setDouble(42, Net_Revenue__c);
      preparedStatement1.setString(43, Notes__c);
      preparedStatement1.setDouble(44, Number_of_Design_Wins__c);
      preparedStatement1.setDouble(45, Number_of_Months_Lifetime__c);
      preparedStatement1.setDouble(46, Opportunity_Stage_Value_Rollup__c);
      preparedStatement1.setBoolean(47, Private_Project__c);
      preparedStatement1.setString(48, Project_ID__c);
      preparedStatement1.setString(49, Standardized_Project_Name_Indexed__c);
      preparedStatement1.setString(50, Purpose__c);
      preparedStatement1.setString(51, Region__c);
      preparedStatement1.setString(52, Related_Project__c);
      preparedStatement1.setString(53, Stage__c);
      preparedStatement1.setString(54, End_Market_Segmentation__c);
      preparedStatement1.setBoolean(55, Flagship__c);
      preparedStatement1.setString(56, Standard_Project_Stage__c);
      preparedStatement1.setString(57, Standardized_NRG_Project_Name__c);
      preparedStatement1.setString(58, Standardized_Project_Name__c);
      preparedStatement1.setString(59, Project_Owner_Name__c);
      preparedStatement1.setString(60, Type__c);
      preparedStatement1.setString(61, RFQ__c);
      preparedStatement1.setString(62, OEM_1__c);
      preparedStatement1.setString(63, OEM_2__c);
      preparedStatement1.setBoolean(64, Discrete_Touch__c);
      preparedStatement1.setBoolean(65, Small_Display__c);
      preparedStatement1.setBoolean(66, Large_Display__c);
      preparedStatement1.setBoolean(67, Fingerprint__c);
      preparedStatement1.setBoolean(68, Touchpad__c);
      preparedStatement1.setBoolean(69, Other__c);
      preparedStatement1.setString(70, Automotive_OEM_Account__c);
      preparedStatement1.setBoolean(71, Multimedia__c);
      preparedStatement1.executeUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void queryApttusAPTSAgreement__c() {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1.prepareStatement("Truncate Table AGREEMENT__C");
      preparedStatement1.executeUpdate();
      QueryResult queryResults = sourceConnection
          .query("Select Id, OwnerId, IsDeleted, Name, RecordTypeId, CreatedDate, CreatedById, "
              + "LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, "
              + "LastViewedDate, LastReferencedDate, Apttus__Account_Search_Field__c, "
              + "Apttus__Account__c, Apttus__Activated_By__c, Apttus__Activated_Date__c, "
              + "Apttus__Agreement_Category__c, Apttus__Agreement_Number__c, "
              + "Apttus__AllowableOutputFormats__c, Apttus__Amendment_Effective_Date__c, "
              + "Apttus__Auto_Renew_Consent__c, Apttus__Auto_Renew_Term_Months__c, "
              + "Apttus__Auto_Renewal_Terms__c, Apttus__Auto_Renewal__c,"
              + " Apttus__Business_Hours__c, "
              + "Apttus__Company_Signed_By__c, Apttus__Company_Signed_Date__c, "
              + "Apttus__Company_Signed_Title__c, Apttus__Contract_Duration_Days__c, "
              + "Apttus__Contract_End_Date__c, Apttus__Contract_Number__c, "
              + "Apttus__Contract_Start_Date__c, Apttus__Contracted_Days__c, "
              + "Apttus__Description__c, Apttus__Executed_Copy_Mailed_Out_Date__c, "
              + "Apttus__FF_Agreement_Number__c, Apttus__FF_Amend__c, "
              + "Apttus__FF_Cancel_Request__c, Apttus__FF_Execute__c, Apttus__FF_Expire__c, "
              + "Apttus__FF_Generate_Protected_Agreement__c, "
              + "Apttus__FF_Generate_Supporting_Document__c, "
              + "Apttus__FF_Generate_Unprotected_Agreement__c, "
              + "Apttus__FF_Regenerate_Agreement__c, Apttus__FF_Renew__c, "
              + "Apttus__FF_Return_To_Requestor__c, "
              + "Apttus__FF_Send_To_Other_Party_For_Review__c, "
              + "Apttus__FF_Send_To_Other_Party_For_Signatures__c,"
              + " Apttus__FF_Send_To_Third_Party__c, Apttus__FF_Submit_For_Changes__c, "
              + "Apttus__FF_Submit_Request__c,"
              + " Apttus__FF_Terminate__c, Apttus__FF_View_Draft_Contract__c, "
              + "Apttus__FF_View_Final_Contract__c, Apttus__Import_Offline_Document__c, "
              + "Apttus__InitiateTermination__c, Apttus__Initiation_Type__c, "
              + "Apttus__Internal_Renewal_Notification_Days__c, "
              + "Apttus__Internal_Renewal_Start_Date__c,"
              + " Apttus__IsInternalReview__c, Apttus__IsLocked__c, Apttus__Is_System_Update__c, "
              + "Apttus__LatestDocId__c, Apttus__Non_Standard_Legal_Language__c, "
              + "Apttus__Other_Party_Returned_Date__c, Apttus__Other_Party_Sent_Date__c, "
              + "Apttus__Other_Party_Signed_By_Unlisted__c, Apttus__Other_Party_Signed_By__c, "
              + "Apttus__Other_Party_Signed_Date__c, Apttus__Other_Party_Signed_Title__c, "
              + "Apttus__Outstanding_Days__c, Apttus__Owner_Expiration_Notice__c, "
              + "Apttus__Parent_Agreement__c, Apttus__Perpetual__c, Apttus__Primary_Contact__c, "
              + "Apttus__Related_Opportunity__c, Apttus__Remaining_Contracted_Days__c, "
              + "Apttus__Renewal_Notice_Date__c, Apttus__Renewal_Notice_Days__c, "
              + "Apttus__Request_Date__c, Apttus__Requestor__c, Apttus__RetentionDate__c, "
              + "Apttus__RetentionPolicyId__c, Apttus__Risk_Rating__c, Apttus__Source__c, "
              + "Apttus__Special_Terms__c, Apttus__Status_Category__c, Apttus__Status__c, "
              + "Apttus__Submit_Request_Mode__c, Apttus__Subtype__c, Apttus__Term_Months__c, "
              + "Apttus__TerminationComments__c, Apttus__Termination_Date__c, "
              + "Apttus__Termination_Notice_Days__c, Apttus__Termination_Notice_Issue_Date__c, "
              + "Apttus__Total_Contract_Value__c, Apttus__VersionAware__c, "
              + "Apttus__Version_Number__c, "
              + "Apttus__Version__c, Apttus__Workflow_Trigger_Created_From_Clone__c, "
              + "Apttus__Workflow_Trigger_Viewed_Final__c, Apttus_Approval__Approval_Status__c, "
              + "Apttus_Approval__LineItem_Approval_Status__c, "
              + "Apttus_Approval__Term_Exception_Approval_Status__c, "
              + "Apttus_CMConfig__AutoActivateOrder__c, Apttus_CMConfig__AutoCreateBill__c,"
              + " Apttus_CMConfig__AutoCreateRevenue__c, Apttus_CMConfig__BillToAccountId__c"
              + " Apttus_CMConfig__BillingPreferenceId__c, "
              + "Apttus_CMConfig__ConfigurationFinalizedDate__c, "
              + "Apttus_CMConfig__ConfigurationSyncDate__c, "
              + "Apttus_CMConfig__Configure__c, Apttus_CMConfig__LocationId__c, "
              + "Apttus_CMConfig__PODate__c, "
              + "Apttus_CMConfig__PONumber__c, Apttus_CMConfig__PaymentTermId__c, "
              + "Apttus_CMConfig__PayoutFrequency__c, "
              + "Apttus_CMConfig__PriceListId__c, Apttus_CMConfig__PricingDate__c, "
              + "Apttus_CMConfig__ReadyForActivationDate__c,"
              + " Apttus_CMConfig__ReadyForBillingDate__c, "
              + "Apttus_CMConfig__ReadyForFulfillmentDate__c, "
              + "Apttus_CMConfig__ReadyForRevRecDate__c, "
              + "Apttus_CMConfig__ShipToAccountId__c, Apttus_CMConfig__SyncWithOpportunity__c, "
              + "Apttus_CMDSign__CheckESignatureStatus__c, Apttus_CMDSign__DocuSignEnvelopeId__c, "
              + "Apttus_CMDSign__RecallESignatureRequest__c, Apttus_CMDSign__SendForESignature__c, "
              + "Apttus_CMDSign__ViewESignatureDocument__c, Apttus_Rebate__ConfigureRebate__c, "
              + "APTS_ActivateButton__c, APTS_Amend__c, APTS_Associate_Part_Numbers__c, "
              + "APTS_First_Customer_PO_Number__c, APTS_Generate__c, "
              + "APTS_Import_Fully_Signed_Document__c, "
              + "APTS_Present__c, APTS_Quote_Actual_Design_Win_Date__c, APTS_Renew__c, "
              + "APTS_Terminate__c, Product_Division__c, APTS_Agreement_Type__c, "
              + "APTS_Lost__c, AVAP_OEM__c, Data_Migration__c, "
              + "APTS_Approval_Status__c, APTS_AcceptAgreement__c, "
              + "APTS_Associate_Part_NumberOPA__c, "
              + "OPA_Lifetime_Product_Forecast__c, Panel__c, APTS_Has_Opportunity__c, "
              + "IDH__c, APTS_Activate_OA_wo_Incentive__c, APTS_Agreement_Benefit_Type__c, "
              + "APTS_Agreement_Incentive_Count__c, APTS_Cancel_Agreement__c, "
              + "APTS_Is_Rebate_involved__c, "
              + "APTS_Opportunity_Family__c, APTS_Product_Division_s__c,"
              + " APTS_Reference_OPA_MPA__c, "
              + "APTS_Related_Account_Agreement__c, SYN_Bill_To_Customer__c, "
              + "OEM_Rebate_Agreement__c, "
              + "Specific_Bill_to_customer__c, Count_Of_OPSC__c FROM Apttus__APTS_Agreement__c ");
      boolean done = false;
      if (queryResults.getSize() > 0) {
        while (!done) {
          for (int i = 0; i < queryResults.getRecords().length; i++) {
            Apttus__APTS_Agreement__c c = (Apttus__APTS_Agreement__c) queryResults.getRecords()[i];
            String Id = c.getId();
            String OwnerId = c.getOwnerId();
            Boolean IsDeleted = c.getIsDeleted();
            String Name = c.getName();
            String RecordTypeId = c.getRecordTypeId();
            Timestamp CreatedDate = handleTimestamp(c.getCreatedDate());
            String CreatedById = c.getCreatedById();
            Timestamp LastModifiedDate = handleTimestamp(c.getLastModifiedDate());
            String LastModifiedById = c.getLastModifiedById();
            Timestamp SystemModstamp = handleTimestamp(c.getSystemModstamp());
            Calendar LastActivityDate_1 = c.getLastActivityDate();
            Date LastActivityDate = handleDate(LastActivityDate_1);
            Timestamp LastViewedDate = handleTimestamp(c.getLastViewedDate());
            Timestamp LastReferencedDate = handleTimestamp(c.getLastReferencedDate());
            String Apttus__Account_Search_Field__c = c.getApttus__Account_Search_Field__c();
            String Apttus__Account__c = c.getApttus__Account__c();
            String Apttus__Activated_By__c = c.getApttus__Activated_By__c();
            Calendar Apttus__Activated_Date__c_1 = c.getApttus__Activated_Date__c();
            Date Apttus__Activated_Date__c = handleDate(Apttus__Activated_Date__c_1);
            String Apttus__Agreement_Category__c = c.getApttus__Agreement_Category__c();
            String Apttus__Agreement_Number__c = c.getApttus__Agreement_Number__c();
            String Apttus__AllowableOutputFormats__c = c.getApttus__AllowableOutputFormats__c();
            Calendar Apttus__Amendment_Effective_Date__c_1 =
                c.getApttus__Amendment_Effective_Date__c();
            Date Apttus__Amendment_Effective_Date__c =
                handleDate(Apttus__Amendment_Effective_Date__c_1);
            Boolean Apttus__Auto_Renew_Consent__c = c.getApttus__Auto_Renew_Consent__c();
            Double Apttus__Auto_Renew_Term_Months__c_1 = c.getApttus__Auto_Renew_Term_Months__c();
            Double Apttus__Auto_Renew_Term_Months__c =
                handleDouble(Apttus__Auto_Renew_Term_Months__c_1);
            String Apttus__Auto_Renewal_Terms__c = c.getApttus__Auto_Renewal_Terms__c();
            Boolean Apttus__Auto_Renewal__c = c.getApttus__Auto_Renewal__c();
            String Apttus__Business_Hours__c = c.getApttus__Business_Hours__c();
            String Apttus__Company_Signed_By__c = c.getApttus__Company_Signed_By__c();
            Calendar Apttus__Company_Signed_Date__c_1 = c.getApttus__Company_Signed_Date__c();
            Date Apttus__Company_Signed_Date__c = handleDate(Apttus__Company_Signed_Date__c_1);
            String Apttus__Company_Signed_Title__c = c.getApttus__Company_Signed_Title__c();
            Double Apttus__Contract_Duration_Days__c_1 = c.getApttus__Contract_Duration_Days__c();
            Double Apttus__Contract_Duration_Days__c =
                handleDouble(Apttus__Contract_Duration_Days__c_1);
            Calendar Apttus__Contract_End_Date__c_1 = c.getApttus__Contract_End_Date__c();
            Date Apttus__Contract_End_Date__c = handleDate(Apttus__Contract_End_Date__c_1);
            String Apttus__Contract_Number__c = c.getApttus__Contract_Number__c();
            Calendar Apttus__Contract_Start_Date__c_1 = c.getApttus__Contract_Start_Date__c();
            Date Apttus__Contract_Start_Date__c = handleDate(Apttus__Contract_Start_Date__c_1);
            Double Apttus__Contracted_Days__c_1 = c.getApttus__Contracted_Days__c();
            Double Apttus__Contracted_Days__c = handleDouble(Apttus__Contracted_Days__c_1);
            String Apttus__Description__c = c.getApttus__Description__c();
            Calendar Apttus__Executed_Copy_Mailed_Out_Date__c_1 =
                c.getApttus__Executed_Copy_Mailed_Out_Date__c();
            Date Apttus__Executed_Copy_Mailed_Out_Date__c =
                handleDate(Apttus__Executed_Copy_Mailed_Out_Date__c_1);
            String Apttus__FF_Agreement_Number__c = c.getApttus__FF_Agreement_Number__c();
            String Apttus__FF_Amend__c = null;
            String Apttus__FF_Cancel_Request__c = null;
            String Apttus__FF_Execute__c = c.getApttus__FF_Execute__c();
            String Apttus__FF_Expire__c = null;
            String Apttus__FF_Generate_Protected_Agreement__c = null;
            String Apttus__FF_Generate_Supporting_Document__c = null;
            String Apttus__FF_Generate_Unprotected_Agreement__c = null;
            String Apttus__FF_Regenerate_Agreement__c = null;
            String Apttus__FF_Renew__c = null;
            String Apttus__FF_Return_To_Requestor__c = null;
            String Apttus__FF_Send_To_Other_Party_For_Review__c = null;
            String Apttus__FF_Send_To_Other_Party_For_Signatures__c = null;
            String Apttus__FF_Send_To_Third_Party__c = c.getApttus__FF_Send_To_Third_Party__c();
            String Apttus__FF_Submit_For_Changes__c = c.getApttus__FF_Submit_For_Changes__c();
            String Apttus__FF_Submit_Request__c = null;
            String Apttus__FF_Terminate__c = null;
            String Apttus__FF_View_Draft_Contract__c = null;
            String Apttus__FF_View_Final_Contract__c = null;
            String Apttus__Import_Offline_Document__c = null;
            String Apttus__InitiateTermination__c = null;
            String Apttus__Initiation_Type__c = c.getApttus__Initiation_Type__c();
            Double Apttus__Internal_Renewal_Notification_Days__c_1 =
                c.getApttus__Internal_Renewal_Notification_Days__c();
            Double Apttus__Internal_Renewal_Notification_Days__c =
                handleDouble(Apttus__Internal_Renewal_Notification_Days__c_1);
            Calendar Apttus__Internal_Renewal_Start_Date__c_1 =
                c.getApttus__Internal_Renewal_Start_Date__c();
            Date Apttus__Internal_Renewal_Start_Date__c =
                handleDate(Apttus__Internal_Renewal_Start_Date__c_1);
            Boolean Apttus__IsInternalReview__c = c.getApttus__IsInternalReview__c();
            Boolean Apttus__IsLocked__c = c.getApttus__IsLocked__c();
            Boolean Apttus__Is_System_Update__c = c.getApttus__Is_System_Update__c();
            String Apttus__LatestDocId__c = c.getApttus__LatestDocId__c();
            Boolean Apttus__Non_Standard_Legal_Language__c =
                c.getApttus__Non_Standard_Legal_Language__c();
            Calendar Apttus__Other_Party_Returned_Date__c_1 =
                c.getApttus__Other_Party_Returned_Date__c();
            Date Apttus__Other_Party_Returned_Date__c =
                handleDate(Apttus__Other_Party_Returned_Date__c_1);
            Calendar Apttus__Other_Party_Sent_Date__c_1 = c.getApttus__Other_Party_Sent_Date__c();
            Date Apttus__Other_Party_Sent_Date__c = handleDate(Apttus__Other_Party_Sent_Date__c_1);
            String Apttus__Other_Party_Signed_By_Unlisted__c =
                c.getApttus__Other_Party_Signed_By_Unlisted__c();
            String Apttus__Other_Party_Signed_By__c = c.getApttus__Other_Party_Signed_By__c();
            Calendar Apttus__Other_Party_Signed_Date__c_1 =
                c.getApttus__Other_Party_Signed_Date__c();
            Date Apttus__Other_Party_Signed_Date__c =
                handleDate(Apttus__Other_Party_Signed_Date__c_1);
            String Apttus__Other_Party_Signed_Title__c = c.getApttus__Other_Party_Signed_Title__c();
            Double Apttus__Outstanding_Days__c_1 = c.getApttus__Outstanding_Days__c();
            Double Apttus__Outstanding_Days__c = handleDouble(Apttus__Outstanding_Days__c_1);
            String Apttus__Owner_Expiration_Notice__c = c.getApttus__Owner_Expiration_Notice__c();
            String Apttus__Parent_Agreement__c = c.getApttus__Parent_Agreement__c();
            Boolean Apttus__Perpetual__c = c.getApttus__Perpetual__c();
            String Apttus__Primary_Contact__c = c.getApttus__Primary_Contact__c();
            String Apttus__Related_Opportunity__c = c.getApttus__Related_Opportunity__c();
            Double Apttus__Remaining_Contracted_Days__c_1 =
                c.getApttus__Remaining_Contracted_Days__c();
            Double Apttus__Remaining_Contracted_Days__c =
                handleDouble(Apttus__Remaining_Contracted_Days__c_1);
            Calendar Apttus__Renewal_Notice_Date__c_1 = c.getApttus__Renewal_Notice_Date__c();
            Date Apttus__Renewal_Notice_Date__c = handleDate(Apttus__Renewal_Notice_Date__c_1);
            Double Apttus__Renewal_Notice_Days__c_1 = c.getApttus__Renewal_Notice_Days__c();
            Double Apttus__Renewal_Notice_Days__c = handleDouble(Apttus__Renewal_Notice_Days__c_1);
            Calendar Apttus__Request_Date__c_1 = c.getApttus__Request_Date__c();
            Date Apttus__Request_Date__c = handleDate(Apttus__Request_Date__c_1);
            String Apttus__Requestor__c = c.getApttus__Requestor__c();
            Calendar Apttus__RetentionDate__c_1 = c.getApttus__RetentionDate__c();
            Date Apttus__RetentionDate__c = handleDate(Apttus__RetentionDate__c_1);
            String Apttus__RetentionPolicyId__c = c.getApttus__RetentionPolicyId__c();
            Double Apttus__Risk_Rating__c_1 = c.getApttus__Risk_Rating__c();
            Double Apttus__Risk_Rating__c = handleDouble(Apttus__Risk_Rating__c_1);
            String Apttus__Source__c = c.getApttus__Source__c();
            String Apttus__Special_Terms__c = c.getApttus__Special_Terms__c();
            String Apttus__Status_Category__c = c.getApttus__Status_Category__c();
            String Apttus__Status__c = c.getApttus__Status__c();
            String Apttus__Submit_Request_Mode__c = c.getApttus__Submit_Request_Mode__c();
            String Apttus__Subtype__c = c.getApttus__Subtype__c();
            Double Apttus__Term_Months__c_1 = c.getApttus__Term_Months__c();
            Double Apttus__Term_Months__c = handleDouble(Apttus__Term_Months__c_1);
            String Apttus__TerminationComments__c = c.getApttus__TerminationComments__c();
            Calendar Apttus__Termination_Date__c_1 = c.getApttus__Termination_Date__c();
            Date Apttus__Termination_Date__c = handleDate(Apttus__Termination_Date__c_1);
            Double Apttus__Termination_Notice_Days__c_1 = c.getApttus__Termination_Notice_Days__c();
            Double Apttus__Termination_Notice_Days__c =
                handleDouble(Apttus__Termination_Notice_Days__c_1);
            Calendar Apttus__Termination_Notice_Issue_Date__c_1 =
                c.getApttus__Termination_Notice_Issue_Date__c();
            Date Apttus__Termination_Notice_Issue_Date__c =
                handleDate(Apttus__Termination_Notice_Issue_Date__c_1);
            Double Apttus__Total_Contract_Value__c_1 = c.getApttus__Total_Contract_Value__c();
            Double Apttus__Total_Contract_Value__c =
                handleDouble(Apttus__Total_Contract_Value__c_1);
            Boolean Apttus__VersionAware__c = c.getApttus__VersionAware__c();
            Double Apttus__Version_Number__c_1 = c.getApttus__Version_Number__c();
            Double Apttus__Version_Number__c = handleDouble(Apttus__Version_Number__c_1);
            Double Apttus__Version__c_1 = c.getApttus__Version__c();
            Double Apttus__Version__c = handleDouble(Apttus__Version__c_1);
            Boolean Apttus__Workflow_Trigger_Created_From_Clone__c =
                c.getApttus__Workflow_Trigger_Created_From_Clone__c();
            Boolean Apttus__Workflow_Trigger_Viewed_Final__c =
                c.getApttus__Workflow_Trigger_Viewed_Final__c();
            String Apttus_Approval__Approval_Status__c = c.getApttus_Approval__Approval_Status__c();
            String Apttus_Approval__LineItem_Approval_Status__c =
                c.getApttus_Approval__LineItem_Approval_Status__c();
            String Apttus_Approval__Term_Exception_Approval_Status__c =
                c.getApttus_Approval__Term_Exception_Approval_Status__c();
            Boolean Apttus_CMConfig__AutoActivateOrder__c =
                c.getApttus_CMConfig__AutoActivateOrder__c();
            Boolean Apttus_CMConfig__AutoCreateBill__c = c.getApttus_CMConfig__AutoCreateBill__c();
            Boolean Apttus_CMConfig__AutoCreateRevenue__c =
                c.getApttus_CMConfig__AutoCreateRevenue__c();
            String Apttus_CMConfig__BillToAccountId__c = c.getApttus_CMConfig__BillToAccountId__c();
            String Apttus_CMConfig__BillingPreferenceId__c =
                c.getApttus_CMConfig__BillingPreferenceId__c();
            Calendar Apttus_CMConfig__ConfigurationFinalizedDate__c_1 =
                c.getApttus_CMConfig__ConfigurationFinalizedDate__c();
            Timestamp Apttus_CMConfig__ConfigurationFinalizedDate__c =
                handleTimestamp(Apttus_CMConfig__ConfigurationFinalizedDate__c_1);
            Calendar Apttus_CMConfig__ConfigurationSyncDate__c_1 =
                c.getApttus_CMConfig__ConfigurationSyncDate__c();
            Timestamp Apttus_CMConfig__ConfigurationSyncDate__c =
                handleTimestamp(Apttus_CMConfig__ConfigurationSyncDate__c_1);
            String Apttus_CMConfig__Configure__c = c.getApttus_CMConfig__Configure__c();
            String Apttus_CMConfig__LocationId__c = c.getApttus_CMConfig__LocationId__c();
            Calendar Apttus_CMConfig__PODate__c_1 = c.getApttus_CMConfig__PODate__c();
            Date Apttus_CMConfig__PODate__c = handleDate(Apttus_CMConfig__PODate__c_1);
            String Apttus_CMConfig__PONumber__c = c.getApttus_CMConfig__PONumber__c();
            String Apttus_CMConfig__PaymentTermId__c = c.getApttus_CMConfig__PaymentTermId__c();
            String Apttus_CMConfig__PayoutFrequency__c = c.getApttus_CMConfig__PayoutFrequency__c();
            String Apttus_CMConfig__PriceListId__c = c.getApttus_CMConfig__PriceListId__c();
            Calendar Apttus_CMConfig__PricingDate__c_1 = c.getApttus_CMConfig__PricingDate__c();
            Timestamp Apttus_CMConfig__PricingDate__c =
                handleTimestamp(Apttus_CMConfig__PricingDate__c_1);
            Calendar Apttus_CMConfig__ReadyForActivationDate__c_1 =
                c.getApttus_CMConfig__ReadyForActivationDate__c();
            Timestamp Apttus_CMConfig__ReadyForActivationDate__c =
                handleTimestamp(Apttus_CMConfig__ReadyForActivationDate__c_1);
            Calendar Apttus_CMConfig__ReadyForBillingDate__c_1 =
                c.getApttus_CMConfig__ReadyForBillingDate__c();
            Timestamp Apttus_CMConfig__ReadyForBillingDate__c =
                handleTimestamp(Apttus_CMConfig__ReadyForBillingDate__c_1);
            Calendar Apttus_CMConfig__ReadyForFulfillmentDate__c_1 =
                c.getApttus_CMConfig__ReadyForFulfillmentDate__c();
            Timestamp Apttus_CMConfig__ReadyForFulfillmentDate__c =
                handleTimestamp(Apttus_CMConfig__ReadyForFulfillmentDate__c_1);
            Calendar Apttus_CMConfig__ReadyForRevRecDate__c_1 =
                c.getApttus_CMConfig__ReadyForRevRecDate__c();
            Timestamp Apttus_CMConfig__ReadyForRevRecDate__c =
                handleTimestamp(Apttus_CMConfig__ReadyForRevRecDate__c_1);
            String Apttus_CMConfig__ShipToAccountId__c = c.getApttus_CMConfig__ShipToAccountId__c();
            String Apttus_CMConfig__SyncWithOpportunity__c = null;
            String Apttus_CMDSign__CheckESignatureStatus__c =
                c.getApttus_CMDSign__CheckESignatureStatus__c();
            String Apttus_CMDSign__DocuSignEnvelopeId__c =
                c.getApttus_CMDSign__DocuSignEnvelopeId__c();
            String Apttus_CMDSign__RecallESignatureRequest__c =
                c.getApttus_CMDSign__RecallESignatureRequest__c();
            String Apttus_CMDSign__SendForESignature__c = null;
            String Apttus_CMDSign__ViewESignatureDocument__c =
                c.getApttus_CMDSign__ViewESignatureDocument__c();
            String Apttus_Rebate__ConfigureRebate__c = c.getApttus_Rebate__ConfigureRebate__c();
            String APTS_ActivateButton__c = null;
            String APTS_Amend__c = null;
            String APTS_Associate_Part_Numbers__c = null;
            String APTS_First_Customer_PO_Number__c = c.getAPTS_First_Customer_PO_Number__c();
            String APTS_Generate__c = null;
            String APTS_Import_Fully_Signed_Document__c =
                c.getAPTS_Import_Fully_Signed_Document__c();
            String APTS_Present__c = null;
            Calendar APTS_Quote_Actual_Design_Win_Date__c_1 =
                c.getAPTS_Quote_Actual_Design_Win_Date__c();
            Date APTS_Quote_Actual_Design_Win_Date__c =
                handleDate(APTS_Quote_Actual_Design_Win_Date__c_1);
            String APTS_Renew__c = null;
            String APTS_Terminate__c = null;
            String Product_Division__c = c.getProduct_Division__c();
            String APTS_Agreement_Type__c = c.getAPTS_Agreement_Type__c();
            String APTS_Lost__c = null;
            Boolean AVAP_OEM__c = c.getAVAP_OEM__c();
            Boolean Data_Migration__c = c.getData_Migration__c();
            String APTS_Approval_Status__c = c.getAPTS_Approval_Status__c();
            String APTS_AcceptAgreement__c = c.getAPTS_AcceptAgreement__c();
            String APTS_Associate_Part_NumberOPA__c = null;
            Double OPA_Lifetime_Product_Forecast__c_1 = c.getOPA_Lifetime_Product_Forecast__c();
            Double OPA_Lifetime_Product_Forecast__c =
                handleDouble(OPA_Lifetime_Product_Forecast__c_1);
            String Panel__c = c.getPanel__c();
            Boolean APTS_Has_Opportunity__c = c.getAPTS_Has_Opportunity__c();
            String IDH__c = c.getIDH__c();
            String APTS_Activate_OA_wo_Incentive__c = null;
            String APTS_Agreement_Benefit_Type__c = c.getAPTS_Agreement_Benefit_Type__c();
            Double APTS_Agreement_Incentive_Count__c_1 = c.getAPTS_Agreement_Incentive_Count__c();
            Double APTS_Agreement_Incentive_Count__c =
                handleDouble(APTS_Agreement_Incentive_Count__c_1);
            String APTS_Cancel_Agreement__c = null;
            Boolean APTS_Is_Rebate_involved__c = c.getAPTS_Is_Rebate_involved__c();
            String APTS_Opportunity_Family__c = c.getAPTS_Opportunity_Family__c();
            String APTS_Product_Division_s__c = c.getAPTS_Product_Division_s__c();
            String APTS_Reference_OPA_MPA__c = c.getAPTS_Reference_OPA_MPA__c();
            String APTS_Related_Account_Agreement__c = c.getAPTS_Related_Account_Agreement__c();
            String SYN_Bill_To_Customer__c = c.getSYN_Bill_To_Customer__c();
            String OEM_Rebate_Agreement__c = c.getOEM_Rebate_Agreement__c();
            String Specific_Bill_to_customer__c = c.getSpecific_Bill_to_customer__c();
            Double Count_Of_OPSC__c_1 = c.getCount_Of_OPSC__c();
            Double Count_Of_OPSC__c = handleDouble(Count_Of_OPSC__c_1);
            insertApttusAPTSAgreement__c(Id, OwnerId, IsDeleted, Name, RecordTypeId, CreatedDate,
                CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate,
                LastViewedDate, LastReferencedDate, Apttus__Account_Search_Field__c,
                Apttus__Account__c, Apttus__Activated_By__c, Apttus__Activated_Date__c,
                Apttus__Agreement_Category__c, Apttus__Agreement_Number__c,
                Apttus__AllowableOutputFormats__c, Apttus__Amendment_Effective_Date__c,
                Apttus__Auto_Renew_Consent__c, Apttus__Auto_Renew_Term_Months__c,
                Apttus__Auto_Renewal_Terms__c, Apttus__Auto_Renewal__c, Apttus__Business_Hours__c,
                Apttus__Company_Signed_By__c, Apttus__Company_Signed_Date__c,
                Apttus__Company_Signed_Title__c, Apttus__Contract_Duration_Days__c,
                Apttus__Contract_End_Date__c, Apttus__Contract_Number__c,
                Apttus__Contract_Start_Date__c, Apttus__Contracted_Days__c, Apttus__Description__c,
                Apttus__Executed_Copy_Mailed_Out_Date__c, Apttus__FF_Agreement_Number__c,
                Apttus__FF_Amend__c, Apttus__FF_Cancel_Request__c, Apttus__FF_Execute__c,
                Apttus__FF_Expire__c, Apttus__FF_Generate_Protected_Agreement__c,
                Apttus__FF_Generate_Supporting_Document__c,
                Apttus__FF_Generate_Unprotected_Agreement__c, Apttus__FF_Regenerate_Agreement__c,
                Apttus__FF_Renew__c, Apttus__FF_Return_To_Requestor__c,
                Apttus__FF_Send_To_Other_Party_For_Review__c,
                Apttus__FF_Send_To_Other_Party_For_Signatures__c, Apttus__FF_Send_To_Third_Party__c,
                Apttus__FF_Submit_For_Changes__c, Apttus__FF_Submit_Request__c,
                Apttus__FF_Terminate__c, Apttus__FF_View_Draft_Contract__c,
                Apttus__FF_View_Final_Contract__c, Apttus__Import_Offline_Document__c,
                Apttus__InitiateTermination__c, Apttus__Initiation_Type__c,
                Apttus__Internal_Renewal_Notification_Days__c,
                Apttus__Internal_Renewal_Start_Date__c, Apttus__IsInternalReview__c,
                Apttus__IsLocked__c, Apttus__Is_System_Update__c, Apttus__LatestDocId__c,
                Apttus__Non_Standard_Legal_Language__c, Apttus__Other_Party_Returned_Date__c,
                Apttus__Other_Party_Sent_Date__c, Apttus__Other_Party_Signed_By_Unlisted__c,
                Apttus__Other_Party_Signed_By__c, Apttus__Other_Party_Signed_Date__c,
                Apttus__Other_Party_Signed_Title__c, Apttus__Outstanding_Days__c,
                Apttus__Owner_Expiration_Notice__c, Apttus__Parent_Agreement__c,
                Apttus__Perpetual__c, Apttus__Primary_Contact__c, Apttus__Related_Opportunity__c,
                Apttus__Remaining_Contracted_Days__c, Apttus__Renewal_Notice_Date__c,
                Apttus__Renewal_Notice_Days__c, Apttus__Request_Date__c, Apttus__Requestor__c,
                Apttus__RetentionDate__c, Apttus__RetentionPolicyId__c, Apttus__Risk_Rating__c,
                Apttus__Source__c, Apttus__Special_Terms__c, Apttus__Status_Category__c,
                Apttus__Status__c, Apttus__Submit_Request_Mode__c, Apttus__Subtype__c,
                Apttus__Term_Months__c, Apttus__TerminationComments__c, Apttus__Termination_Date__c,
                Apttus__Termination_Notice_Days__c, Apttus__Termination_Notice_Issue_Date__c,
                Apttus__Total_Contract_Value__c, Apttus__VersionAware__c, Apttus__Version_Number__c,
                Apttus__Version__c, Apttus__Workflow_Trigger_Created_From_Clone__c,
                Apttus__Workflow_Trigger_Viewed_Final__c, Apttus_Approval__Approval_Status__c,
                Apttus_Approval__LineItem_Approval_Status__c,
                Apttus_Approval__Term_Exception_Approval_Status__c,
                Apttus_CMConfig__AutoActivateOrder__c, Apttus_CMConfig__AutoCreateBill__c,
                Apttus_CMConfig__AutoCreateRevenue__c, Apttus_CMConfig__BillToAccountId__c,
                Apttus_CMConfig__BillingPreferenceId__c,
                Apttus_CMConfig__ConfigurationFinalizedDate__c,
                Apttus_CMConfig__ConfigurationSyncDate__c, Apttus_CMConfig__Configure__c,
                Apttus_CMConfig__LocationId__c, Apttus_CMConfig__PODate__c,
                Apttus_CMConfig__PONumber__c, Apttus_CMConfig__PaymentTermId__c,
                Apttus_CMConfig__PayoutFrequency__c, Apttus_CMConfig__PriceListId__c,
                Apttus_CMConfig__PricingDate__c, Apttus_CMConfig__ReadyForActivationDate__c,
                Apttus_CMConfig__ReadyForBillingDate__c,
                Apttus_CMConfig__ReadyForFulfillmentDate__c, Apttus_CMConfig__ReadyForRevRecDate__c,
                Apttus_CMConfig__ShipToAccountId__c, Apttus_CMConfig__SyncWithOpportunity__c,
                Apttus_CMDSign__CheckESignatureStatus__c, Apttus_CMDSign__DocuSignEnvelopeId__c,
                Apttus_CMDSign__RecallESignatureRequest__c, Apttus_CMDSign__SendForESignature__c,
                Apttus_CMDSign__ViewESignatureDocument__c, Apttus_Rebate__ConfigureRebate__c,
                APTS_ActivateButton__c, APTS_Amend__c, APTS_Associate_Part_Numbers__c,
                APTS_First_Customer_PO_Number__c, APTS_Generate__c,
                APTS_Import_Fully_Signed_Document__c, APTS_Present__c,
                APTS_Quote_Actual_Design_Win_Date__c, APTS_Renew__c, APTS_Terminate__c,
                Product_Division__c, APTS_Agreement_Type__c, APTS_Lost__c, AVAP_OEM__c,
                Data_Migration__c, APTS_Approval_Status__c, APTS_AcceptAgreement__c,
                APTS_Associate_Part_NumberOPA__c, OPA_Lifetime_Product_Forecast__c, Panel__c,
                APTS_Has_Opportunity__c, IDH__c, APTS_Activate_OA_wo_Incentive__c,
                APTS_Agreement_Benefit_Type__c, APTS_Agreement_Incentive_Count__c,
                APTS_Cancel_Agreement__c, APTS_Is_Rebate_involved__c, APTS_Opportunity_Family__c,
                APTS_Product_Division_s__c, APTS_Reference_OPA_MPA__c,
                APTS_Related_Account_Agreement__c, SYN_Bill_To_Customer__c, OEM_Rebate_Agreement__c,
                Specific_Bill_to_customer__c, Count_Of_OPSC__c);
            System.out.println("Inserting into ApttusAPTSAgreement__c Id: " + Id);
          }
          if (queryResults.isDone()) {
            done = true;
          } else {
            queryResults = sourceConnection.queryMore(queryResults.getQueryLocator());
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void insertApttusAPTSAgreement__c(String Id, String OwnerId, Boolean IsDeleted,
      String Name, String RecordTypeId, Timestamp CreatedDate, String CreatedById,
      Timestamp LastModifiedDate, String LastModifiedById, Timestamp SystemModstamp,
      Date LastActivityDate, Timestamp LastViewedDate, Timestamp LastReferencedDate,
      String Apttus__Account_Search_Field__c, String Apttus__Account__c,
      String Apttus__Activated_By__c, Date Apttus__Activated_Date__c,
      String Apttus__Agreement_Category__c, String Apttus__Agreement_Number__c,
      String Apttus__AllowableOutputFormats__c, Date Apttus__Amendment_Effective_Date__c,
      Boolean Apttus__Auto_Renew_Consent__c, Double Apttus__Auto_Renew_Term_Months__c,
      String Apttus__Auto_Renewal_Terms__c, Boolean Apttus__Auto_Renewal__c,
      String Apttus__Business_Hours__c, String Apttus__Company_Signed_By__c,
      Date Apttus__Company_Signed_Date__c, String Apttus__Company_Signed_Title__c,
      Double Apttus__Contract_Duration_Days__c, Date Apttus__Contract_End_Date__c,
      String Apttus__Contract_Number__c, Date Apttus__Contract_Start_Date__c,
      Double Apttus__Contracted_Days__c, String Apttus__Description__c,
      Date Apttus__Executed_Copy_Mailed_Out_Date__c, String Apttus__FF_Agreement_Number__c,
      String Apttus__FF_Amend__c, String Apttus__FF_Cancel_Request__c, String Apttus__FF_Execute__c,
      String Apttus__FF_Expire__c, String Apttus__FF_Generate_Protected_Agreement__c,
      String Apttus__FF_Generate_Supporting_Document__c,
      String Apttus__FF_Generate_Unprotected_Agreement__c,
      String Apttus__FF_Regenerate_Agreement__c, String Apttus__FF_Renew__c,
      String Apttus__FF_Return_To_Requestor__c, String Apttus__FF_Send_To_Other_Party_For_Review__c,
      String Apttus__FF_Send_To_Other_Party_For_Signatures__c,
      String Apttus__FF_Send_To_Third_Party__c, String Apttus__FF_Submit_For_Changes__c,
      String Apttus__FF_Submit_Request__c, String Apttus__FF_Terminate__c,
      String Apttus__FF_View_Draft_Contract__c, String Apttus__FF_View_Final_Contract__c,
      String Apttus__Import_Offline_Document__c, String Apttus__InitiateTermination__c,
      String Apttus__Initiation_Type__c, Double Apttus__Internal_Renewal_Notification_Days__c,
      Date Apttus__Internal_Renewal_Start_Date__c, Boolean Apttus__IsInternalReview__c,
      Boolean Apttus__IsLocked__c, Boolean Apttus__Is_System_Update__c,
      String Apttus__LatestDocId__c, Boolean Apttus__Non_Standard_Legal_Language__c,
      Date Apttus__Other_Party_Returned_Date__c, Date Apttus__Other_Party_Sent_Date__c,
      String Apttus__Other_Party_Signed_By_Unlisted__c, String Apttus__Other_Party_Signed_By__c,
      Date Apttus__Other_Party_Signed_Date__c, String Apttus__Other_Party_Signed_Title__c,
      Double Apttus__Outstanding_Days__c, String Apttus__Owner_Expiration_Notice__c,
      String Apttus__Parent_Agreement__c, Boolean Apttus__Perpetual__c,
      String Apttus__Primary_Contact__c, String Apttus__Related_Opportunity__c,
      Double Apttus__Remaining_Contracted_Days__c, Date Apttus__Renewal_Notice_Date__c,
      Double Apttus__Renewal_Notice_Days__c, Date Apttus__Request_Date__c,
      String Apttus__Requestor__c, Date Apttus__RetentionDate__c,
      String Apttus__RetentionPolicyId__c, Double Apttus__Risk_Rating__c, String Apttus__Source__c,
      String Apttus__Special_Terms__c, String Apttus__Status_Category__c, String Apttus__Status__c,
      String Apttus__Submit_Request_Mode__c, String Apttus__Subtype__c,
      Double Apttus__Term_Months__c, String Apttus__TerminationComments__c,
      Date Apttus__Termination_Date__c, Double Apttus__Termination_Notice_Days__c,
      Date Apttus__Termination_Notice_Issue_Date__c, Double Apttus__Total_Contract_Value__c,
      Boolean Apttus__VersionAware__c, Double Apttus__Version_Number__c, Double Apttus__Version__c,
      Boolean Apttus__Workflow_Trigger_Created_From_Clone__c,
      Boolean Apttus__Workflow_Trigger_Viewed_Final__c, String Apttus_Approval__Approval_Status__c,
      String Apttus_Approval__LineItem_Approval_Status__c,
      String Apttus_Approval__Term_Exception_Approval_Status__c,
      Boolean Apttus_CMConfig__AutoActivateOrder__c, Boolean Apttus_CMConfig__AutoCreateBill__c,
      Boolean Apttus_CMConfig__AutoCreateRevenue__c, String Apttus_CMConfig__BillToAccountId__c,
      String Apttus_CMConfig__BillingPreferenceId__c,
      Timestamp Apttus_CMConfig__ConfigurationFinalizedDate__c,
      Timestamp Apttus_CMConfig__ConfigurationSyncDate__c, String Apttus_CMConfig__Configure__c,
      String Apttus_CMConfig__LocationId__c, Date Apttus_CMConfig__PODate__c,
      String Apttus_CMConfig__PONumber__c, String Apttus_CMConfig__PaymentTermId__c,
      String Apttus_CMConfig__PayoutFrequency__c, String Apttus_CMConfig__PriceListId__c,
      Timestamp Apttus_CMConfig__PricingDate__c,
      Timestamp Apttus_CMConfig__ReadyForActivationDate__c,
      Timestamp Apttus_CMConfig__ReadyForBillingDate__c,
      Timestamp Apttus_CMConfig__ReadyForFulfillmentDate__c,
      Timestamp Apttus_CMConfig__ReadyForRevRecDate__c, String Apttus_CMConfig__ShipToAccountId__c,
      String Apttus_CMConfig__SyncWithOpportunity__c,
      String Apttus_CMDSign__CheckESignatureStatus__c, String Apttus_CMDSign__DocuSignEnvelopeId__c,
      String Apttus_CMDSign__RecallESignatureRequest__c,
      String Apttus_CMDSign__SendForESignature__c, String Apttus_CMDSign__ViewESignatureDocument__c,
      String Apttus_Rebate__ConfigureRebate__c, String APTS_ActivateButton__c, String APTS_Amend__c,
      String APTS_Associate_Part_Numbers__c, String APTS_First_Customer_PO_Number__c,
      String APTS_Generate__c, String APTS_Import_Fully_Signed_Document__c, String APTS_Present__c,
      Date APTS_Quote_Actual_Design_Win_Date__c, String APTS_Renew__c, String APTS_Terminate__c,
      String Product_Division__c, String APTS_Agreement_Type__c, String APTS_Lost__c,
      Boolean AVAP_OEM__c, Boolean Data_Migration__c, String APTS_Approval_Status__c,
      String APTS_AcceptAgreement__c, String APTS_Associate_Part_NumberOPA__c,
      Double OPA_Lifetime_Product_Forecast__c, String Panel__c, Boolean APTS_Has_Opportunity__c,
      String IDH__c, String APTS_Activate_OA_wo_Incentive__c, String APTS_Agreement_Benefit_Type__c,
      Double APTS_Agreement_Incentive_Count__c, String APTS_Cancel_Agreement__c,
      Boolean APTS_Is_Rebate_involved__c, String APTS_Opportunity_Family__c,
      String APTS_Product_Division_s__c, String APTS_Reference_OPA_MPA__c,
      String APTS_Related_Account_Agreement__c, String SYN_Bill_To_Customer__c,
      String OEM_Rebate_Agreement__c, String Specific_Bill_to_customer__c,
      Double Count_Of_OPSC__c) {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1
          .prepareStatement("INSERT INTO AGREEMENT__C(ID, OWNERID, ISDELETED, NAME, RECORDTYPEID, "
              + "CREATEDDATE, CREATEDBYID, LASTMODIFIEDDATE, "
              + "LASTMODIFIEDBYID, SYSTEMMODSTAMP, LASTACTIVITYDATE, LASTVIEWEDDATE, "
              + "LASTREFERENCEDDATE, "
              + "ACCOUNT_SEARCH_FIELD_C, ACCOUNT_C, ACTIVATED_BY_C, ACTIVATED_DATE_C, "
              + "AGREEMENT_CATEGORY_C, AGREEMENT_NUMBER_C, ALLOWABLEOUTPUTFORMATS_C, "
              + "AMENDMENT_EFFECTIVE_DATE_C, AUTO_RENEW_CONSENT_C, "
              + "AUTO_RENEW_TERM_MONTHS_C, AUTO_RENEWAL_TERMS_C, "
              + "AUTO_RENEWAL_C, BUSINESS_HOURS_C, COMPANY_SIGNED_BY_C, "
              + "COMPANY_SIGNED_DATE_C, COMPANY_SIGNED_TITLE_C, "
              + "CONTRACT_DURATION_DAYS_C, CONTRACT_END_DATE_C, CONTRACT_NUMBER_C, "
              + "CONTRACT_START_DATE_C, CONTRACTED_DAYS_C, DESCRIPTION_C, "
              + "EXEC_COPY_MAILED_OUT_DT_C, FF_AGREEMENT_NUMBER_C, "
              + "FF_AMEND_C, FF_CANCEL_REQUEST_C, FF_EXECUTE_C, FF_EXPIRE_C, "
              + "FF_GNRT_PROTECT_AGREEMENT_C, "
              + "FF_GNRT_SUPPORT_DOCUMENT_C, FF_GNRT_UNPROT_AGREEMENT_C, "
              + "FF_REGENERATE_AGREEMENT_C, FF_RENEW_C, "
              + "FF_RETURN_TO_REQUESTOR_C, FF_SND_2_OTHER_PTY_4_REVIEW_C, "
              + "FF_SND_2_OTHER_PTY_4_SIGN_C, FF_SEND_TO_THIRD_PARTY_C, "
              + "FF_SUBMIT_FOR_CHANGES_C, FF_SUBMIT_REQUEST_C, "
              + "FF_TERMINATE_C, FF_VIEW_DRAFT_CONTRACT_C, FF_VIEW_FINAL_CONTRACT_C, "
              + "IMPORT_OFFLINE_DOCUMENT_C, INITIATETERMINATION_C, "
              + "INITIATION_TYPE_C, INTR_RENEWAL_NOTIFI_DAYS_C, INTERNAL_RENEWAL_START_DATE_C, "
              + "ISINTERNALREVIEW_C, ISLOCKED_C, IS_SYSTEM_UPDATE_C, "
              + "LATESTDOCID_C, NON_STANDARD_LEGAL_LANG_C, "
              + "OTHER_PARTY_RETURNED_DATE_C, OTHER_PARTY_SENT_DATE_C, "
              + "OTHR_PTY_SIGN_BY_UNLISTED_C, OTHER_PARTY_SIGNED_BY_C, "
              + "OTHER_PARTY_SIGNED_DATE_C, OTHER_PARTY_SIGNED_TITLE_C, "
              + "OUTSTANDING_DAYS_C, OWNER_EXPIRATION_NOTICE_C, "
              + "PARENT_AGREEMENT_C, PERPETUAL_C, PRIMARY_CONTACT_C, "
              + "RELATED_OPPORTUNITY_C, REMAINING_CONTRACTED_DAYS_C, "
              + "RENEWAL_NOTICE_DATE_C, RENEWAL_NOTICE_DAYS_C, "
              + "REQUEST_DATE_C, REQUESTOR_C, RETENTIONDATE_C, "
              + "RETENTIONPOLICYID_C, RISK_RATING_C, SOURCE_C, SPECIAL_TERMS_C, STATUS_CATEGORY_C, "
              + "STATUS_C, SUBMIT_REQUEST_MODE_C, SUBTYPE_C, TERM_MONTHS_C, TERMINATIONCOMMENTS_C, "
              + "TERMINATION_DATE_C, TERMINATION_NOTICE_DAYS_C, TERMINATI_NOTICE_ISSUE_DT_C, "
              + "TOTAL_CONTRACT_VALUE_C, VERSIONAWARE_C, VERSION_NUMBER_C, "
              + "VERSION_C, WF_TRIG_CREATED_FROM_CLONE_C, WF_TRIGGER_VIEWED_FINAL_C, "
              + "APPROVAL_APPROVAL_STATUS_C, APPR_LINEITEM_APPR_STATUS_C, "
              + "APPR_TRM_EXCEPT_APPR_STATUS_C, AUTOACTIVATEORDER_C, AUTOCREATEBILL_C, "
              + "AUTOCREATEREVENUE_C, BILLTOACCOUNTID_C, BILLINGPREFERENCEID_C, "
              + "CONFGFINALIZEDDT_C, CONFGSYNCDATE_C, CONFIGURE_C, "
              + "LOCATIONID_C, PODATE_C, PONUMBER_C, "
              + "PAYMENTTERMID_C, PAYOUTFREQUENCY_C, PRICELISTID_C, PRICINGDATE_C, "
              + "READYFORACTIVATIONDATE_C, READYFORBILLINGDATE_C, READY4FULFILLMENTDT_C, "
              + "READYFORREVRECDATE_C, SHIPTOACCOUNTID_C, SYNCWITHOPPORTUNITY_C, "
              + "CHECKESIGNATURESTATUS_C, DOCUSIGNENVELOPEID_C, RECALLESIGNATUREREQUEST_C, "
              + "SENDFORESIGNATURE_C, "
              + "VIEWESIGNATUREDOCUMENT_C, REBATE_CONFIGUREREBATE_C, ACTIVATEBUTTON_C, "
              + "AMEND_C, ASSOCIATE_PART_NUMBERS_C, FIRST_CUSTOMER_PO_NUMBER_C, "
              + "GENERATE_C, IMPORT_FULLY_SIGNED_DOCUMENT_C, "
              + "PRESENT_C, QUOTE_ACTUAL_DESIGN_WIN_DATE_C, RENEW_C, TERMINATE_C, "
              + "PRODUCT_DIVISION_C, AGREEMENT_TYPE_C, LOST_C, AVAP_OEM_C, "
              + "DATA_MIGRATION_C, APPROVAL_STATUS_C, ACCEPTAGREEMENT_C, "
              + "ASSOCIATE_PART_NUMBEROPA_C, OPA_LIFETIME_PROD_FCST_C, "
              + "PANEL_C, HAS_OPPORTUNITY_C, IDH_C, ACTIVATE_OA_WO_INCENTIVE_C, "
              + "AGREEMENT_BENEFIT_TYPE_C, AGREEMENT_INCENTIVE_COUNT_C, "
              + "CANCEL_AGREEMENT_C, IS_REBATE_INVOLVED_C, OPPORTUNITY_FAMILY_C, "
              + "PRODUCT_DIVISION_S_C, REFERENCE_OPA_MPA_C, "
              + "RELATED_ACCOUNT_AGREEMENT_C, SYN_BILL_TO_CUSTOMER_C, "
              + "OEM_REBATE_AGREEMENT_C, SPECIFIC_BILL_TO_CUSTOMER_C, COUNT_OF_OPSC__C)"
              + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      preparedStatement1.setString(1, Id);
      preparedStatement1.setString(2, OwnerId);
      preparedStatement1.setBoolean(3, IsDeleted);
      preparedStatement1.setString(4, Name);
      preparedStatement1.setString(5, RecordTypeId);
      preparedStatement1.setTimestamp(6, CreatedDate);
      preparedStatement1.setString(7, CreatedById);
      preparedStatement1.setTimestamp(8, LastModifiedDate);
      preparedStatement1.setString(9, LastModifiedById);
      preparedStatement1.setTimestamp(10, SystemModstamp);
      preparedStatement1.setDate(11, LastActivityDate);
      preparedStatement1.setTimestamp(12, LastViewedDate);
      preparedStatement1.setTimestamp(13, LastReferencedDate);
      preparedStatement1.setString(14, Apttus__Account_Search_Field__c);
      preparedStatement1.setString(15, Apttus__Account__c);
      preparedStatement1.setString(16, Apttus__Activated_By__c);
      preparedStatement1.setDate(17, Apttus__Activated_Date__c);
      preparedStatement1.setString(18, Apttus__Agreement_Category__c);
      preparedStatement1.setString(19, Apttus__Agreement_Number__c);
      preparedStatement1.setString(20, Apttus__AllowableOutputFormats__c);
      preparedStatement1.setDate(21, Apttus__Amendment_Effective_Date__c);
      preparedStatement1.setBoolean(22, Apttus__Auto_Renew_Consent__c);
      preparedStatement1.setDouble(23, Apttus__Auto_Renew_Term_Months__c);
      preparedStatement1.setString(24, Apttus__Auto_Renewal_Terms__c);
      preparedStatement1.setBoolean(25, Apttus__Auto_Renewal__c);
      preparedStatement1.setString(26, Apttus__Business_Hours__c);
      preparedStatement1.setString(27, Apttus__Company_Signed_By__c);
      preparedStatement1.setDate(28, Apttus__Company_Signed_Date__c);
      preparedStatement1.setString(29, Apttus__Company_Signed_Title__c);
      preparedStatement1.setDouble(30, Apttus__Contract_Duration_Days__c);
      preparedStatement1.setDate(31, Apttus__Contract_End_Date__c);
      preparedStatement1.setString(32, Apttus__Contract_Number__c);
      preparedStatement1.setDate(33, Apttus__Contract_Start_Date__c);
      preparedStatement1.setDouble(34, Apttus__Contracted_Days__c);
      preparedStatement1.setString(35, Apttus__Description__c);
      preparedStatement1.setDate(36, Apttus__Executed_Copy_Mailed_Out_Date__c);
      preparedStatement1.setString(37, Apttus__FF_Agreement_Number__c);
      preparedStatement1.setString(38, Apttus__FF_Amend__c);
      preparedStatement1.setString(39, Apttus__FF_Cancel_Request__c);
      preparedStatement1.setString(40, Apttus__FF_Execute__c);
      preparedStatement1.setString(41, Apttus__FF_Expire__c);
      preparedStatement1.setString(42, Apttus__FF_Generate_Protected_Agreement__c);
      preparedStatement1.setString(43, Apttus__FF_Generate_Supporting_Document__c);
      preparedStatement1.setString(44, Apttus__FF_Generate_Unprotected_Agreement__c);
      preparedStatement1.setString(45, Apttus__FF_Regenerate_Agreement__c);
      preparedStatement1.setString(46, Apttus__FF_Renew__c);
      preparedStatement1.setString(47, Apttus__FF_Return_To_Requestor__c);
      preparedStatement1.setString(48, Apttus__FF_Send_To_Other_Party_For_Review__c);
      preparedStatement1.setString(49, Apttus__FF_Send_To_Other_Party_For_Signatures__c);
      preparedStatement1.setString(50, Apttus__FF_Send_To_Third_Party__c);
      preparedStatement1.setString(51, Apttus__FF_Submit_For_Changes__c);
      preparedStatement1.setString(52, Apttus__FF_Submit_Request__c);
      preparedStatement1.setString(53, Apttus__FF_Terminate__c);
      preparedStatement1.setString(54, Apttus__FF_View_Draft_Contract__c);
      preparedStatement1.setString(55, Apttus__FF_View_Final_Contract__c);
      preparedStatement1.setString(56, Apttus__Import_Offline_Document__c);
      preparedStatement1.setString(57, Apttus__InitiateTermination__c);
      preparedStatement1.setString(58, Apttus__Initiation_Type__c);
      preparedStatement1.setDouble(59, Apttus__Internal_Renewal_Notification_Days__c);
      preparedStatement1.setDate(60, Apttus__Internal_Renewal_Start_Date__c);
      preparedStatement1.setBoolean(61, Apttus__IsInternalReview__c);
      preparedStatement1.setBoolean(62, Apttus__IsLocked__c);
      preparedStatement1.setBoolean(63, Apttus__Is_System_Update__c);
      preparedStatement1.setString(64, Apttus__LatestDocId__c);
      preparedStatement1.setBoolean(65, Apttus__Non_Standard_Legal_Language__c);
      preparedStatement1.setDate(66, Apttus__Other_Party_Returned_Date__c);
      preparedStatement1.setDate(67, Apttus__Other_Party_Sent_Date__c);
      preparedStatement1.setString(68, Apttus__Other_Party_Signed_By_Unlisted__c);
      preparedStatement1.setString(69, Apttus__Other_Party_Signed_By__c);
      preparedStatement1.setDate(70, Apttus__Other_Party_Signed_Date__c);
      preparedStatement1.setString(71, Apttus__Other_Party_Signed_Title__c);
      preparedStatement1.setDouble(72, Apttus__Outstanding_Days__c);
      preparedStatement1.setString(73, Apttus__Owner_Expiration_Notice__c);
      preparedStatement1.setString(74, Apttus__Parent_Agreement__c);
      preparedStatement1.setBoolean(75, Apttus__Perpetual__c);
      preparedStatement1.setString(76, Apttus__Primary_Contact__c);
      preparedStatement1.setString(77, Apttus__Related_Opportunity__c);
      preparedStatement1.setDouble(78, Apttus__Remaining_Contracted_Days__c);
      preparedStatement1.setDate(79, Apttus__Renewal_Notice_Date__c);
      preparedStatement1.setDouble(80, Apttus__Renewal_Notice_Days__c);
      preparedStatement1.setDate(81, Apttus__Request_Date__c);
      preparedStatement1.setString(82, Apttus__Requestor__c);
      preparedStatement1.setDate(83, Apttus__RetentionDate__c);
      preparedStatement1.setString(84, Apttus__RetentionPolicyId__c);
      preparedStatement1.setDouble(85, Apttus__Risk_Rating__c);
      preparedStatement1.setString(86, Apttus__Source__c);
      preparedStatement1.setString(87, Apttus__Special_Terms__c);
      preparedStatement1.setString(88, Apttus__Status_Category__c);
      preparedStatement1.setString(89, Apttus__Status__c);
      preparedStatement1.setString(90, Apttus__Submit_Request_Mode__c);
      preparedStatement1.setString(91, Apttus__Subtype__c);
      preparedStatement1.setDouble(92, Apttus__Term_Months__c);
      preparedStatement1.setString(93, Apttus__TerminationComments__c);
      preparedStatement1.setDate(94, Apttus__Termination_Date__c);
      preparedStatement1.setDouble(95, Apttus__Termination_Notice_Days__c);
      preparedStatement1.setDate(96, Apttus__Termination_Notice_Issue_Date__c);
      preparedStatement1.setDouble(97, Apttus__Total_Contract_Value__c);
      preparedStatement1.setBoolean(98, Apttus__VersionAware__c);
      preparedStatement1.setDouble(99, Apttus__Version_Number__c);
      preparedStatement1.setDouble(100, Apttus__Version__c);
      preparedStatement1.setBoolean(101, Apttus__Workflow_Trigger_Created_From_Clone__c);
      preparedStatement1.setBoolean(102, Apttus__Workflow_Trigger_Viewed_Final__c);
      preparedStatement1.setString(103, Apttus_Approval__Approval_Status__c);
      preparedStatement1.setString(104, Apttus_Approval__LineItem_Approval_Status__c);
      preparedStatement1.setString(105, Apttus_Approval__Term_Exception_Approval_Status__c);
      preparedStatement1.setBoolean(106, Apttus_CMConfig__AutoActivateOrder__c);
      preparedStatement1.setBoolean(107, Apttus_CMConfig__AutoCreateBill__c);
      preparedStatement1.setBoolean(108, Apttus_CMConfig__AutoCreateRevenue__c);
      preparedStatement1.setString(109, Apttus_CMConfig__BillToAccountId__c);
      preparedStatement1.setString(110, Apttus_CMConfig__BillingPreferenceId__c);
      preparedStatement1.setTimestamp(111, Apttus_CMConfig__ConfigurationFinalizedDate__c);
      preparedStatement1.setTimestamp(112, Apttus_CMConfig__ConfigurationSyncDate__c);
      preparedStatement1.setString(113, Apttus_CMConfig__Configure__c);
      preparedStatement1.setString(114, Apttus_CMConfig__LocationId__c);
      preparedStatement1.setDate(115, Apttus_CMConfig__PODate__c);
      preparedStatement1.setString(116, Apttus_CMConfig__PONumber__c);
      preparedStatement1.setString(117, Apttus_CMConfig__PaymentTermId__c);
      preparedStatement1.setString(118, Apttus_CMConfig__PayoutFrequency__c);
      preparedStatement1.setString(119, Apttus_CMConfig__PriceListId__c);
      preparedStatement1.setTimestamp(120, Apttus_CMConfig__PricingDate__c);
      preparedStatement1.setTimestamp(121, Apttus_CMConfig__ReadyForActivationDate__c);
      preparedStatement1.setTimestamp(122, Apttus_CMConfig__ReadyForBillingDate__c);
      preparedStatement1.setTimestamp(123, Apttus_CMConfig__ReadyForFulfillmentDate__c);
      preparedStatement1.setTimestamp(124, Apttus_CMConfig__ReadyForRevRecDate__c);
      preparedStatement1.setString(125, Apttus_CMConfig__ShipToAccountId__c);
      preparedStatement1.setString(126, Apttus_CMConfig__SyncWithOpportunity__c);
      preparedStatement1.setString(127, Apttus_CMDSign__CheckESignatureStatus__c);
      preparedStatement1.setString(128, Apttus_CMDSign__DocuSignEnvelopeId__c);
      preparedStatement1.setString(129, Apttus_CMDSign__RecallESignatureRequest__c);
      preparedStatement1.setString(130, Apttus_CMDSign__SendForESignature__c);
      preparedStatement1.setString(131, Apttus_CMDSign__ViewESignatureDocument__c);
      preparedStatement1.setString(132, Apttus_Rebate__ConfigureRebate__c);
      preparedStatement1.setString(133, APTS_ActivateButton__c);
      preparedStatement1.setString(134, APTS_Amend__c);
      preparedStatement1.setString(135, APTS_Associate_Part_Numbers__c);
      preparedStatement1.setString(136, APTS_First_Customer_PO_Number__c);
      preparedStatement1.setString(137, APTS_Generate__c);
      preparedStatement1.setString(138, APTS_Import_Fully_Signed_Document__c);
      preparedStatement1.setString(139, APTS_Present__c);
      preparedStatement1.setDate(140, APTS_Quote_Actual_Design_Win_Date__c);
      preparedStatement1.setString(141, APTS_Renew__c);
      preparedStatement1.setString(142, APTS_Terminate__c);
      preparedStatement1.setString(143, Product_Division__c);
      preparedStatement1.setString(144, APTS_Agreement_Type__c);
      preparedStatement1.setString(145, APTS_Lost__c);
      preparedStatement1.setBoolean(146, AVAP_OEM__c);
      preparedStatement1.setBoolean(147, Data_Migration__c);
      preparedStatement1.setString(148, APTS_Approval_Status__c);
      preparedStatement1.setString(149, APTS_AcceptAgreement__c);
      preparedStatement1.setString(150, APTS_Associate_Part_NumberOPA__c);
      preparedStatement1.setDouble(151, OPA_Lifetime_Product_Forecast__c);
      preparedStatement1.setString(152, Panel__c);
      preparedStatement1.setBoolean(153, APTS_Has_Opportunity__c);
      preparedStatement1.setString(154, IDH__c);
      preparedStatement1.setString(155, APTS_Activate_OA_wo_Incentive__c);
      preparedStatement1.setString(156, APTS_Agreement_Benefit_Type__c);
      preparedStatement1.setDouble(157, APTS_Agreement_Incentive_Count__c);
      preparedStatement1.setString(158, APTS_Cancel_Agreement__c);
      preparedStatement1.setBoolean(159, APTS_Is_Rebate_involved__c);
      preparedStatement1.setString(160, APTS_Opportunity_Family__c);
      preparedStatement1.setString(161, APTS_Product_Division_s__c);
      preparedStatement1.setString(162, APTS_Reference_OPA_MPA__c);
      preparedStatement1.setString(163, APTS_Related_Account_Agreement__c);
      preparedStatement1.setString(164, SYN_Bill_To_Customer__c);
      preparedStatement1.setString(165, OEM_Rebate_Agreement__c);
      preparedStatement1.setString(166, Specific_Bill_to_customer__c);
      preparedStatement1.setDouble(167, Count_Of_OPSC__c);
      preparedStatement1.executeUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void queryAccount() {
    Connection connection1 = null;
    Connection connection2 = null;
    PreparedStatement preparedStatement1 = null;
    PreparedStatement preparedStatement2 = null;
    ResultSet resultSet1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1.prepareStatement("Truncate Table ACCOUNT");
      preparedStatement1.executeUpdate();
      connection2 = getConnection();
      preparedStatement2 = connection2.prepareStatement(
          "select ID, ISDELETED, MASTERRECORDID, NAME, RECORDTYPEID, PARENTID, BILLINGSTREET, "
              + "BILLINGCITY, BILLINGSTATE, BILLINGPOSTALCODE, BILLINGCOUNTRY, BILLINGLATITUDE, "
              + "BILLINGLONGITUDE, BILLINGGEOCODEACCURACY, BILLINGADDRESS, "
              + "SHIPPINGSTREET, SHIPPINGCITY, SHIPPINGSTATE, SHIPPINGPOSTALCODE, SHIPPINGCOUNTRY, "
              + "SHIPPINGLATITUDE, SHIPPINGLONGITUDE, "
              + "SHIPPINGGEOCODEACCURACY, SHIPPINGADDRESS, PHONE, FAX, ACCOUNTNUMBER, WEBSITE, "
              + "PHOTOURL, INDUSTRY, ANNUALREVENUE, "
              + "NUMBEROFEMPLOYEES, DESCRIPTION, SITE, OWNERID, CREATEDDATE, CREATEDBYID, "
              + "LASTMODIFIEDDATE, LASTMODIFIEDBYID, SYSTEMMODSTAMP, "
              + "LASTACTIVITYDATE, LASTVIEWEDDATE, LASTREFERENCEDDATE, JIGSAW, JIGSAWCOMPANYID, "
              + "ACCOUNTSOURCE, SICDESC, REGION__C, "
              + "ACCOUNT_UNIQUE_KEY__C, PARENT_ACCOUNT_NUMBER__C, ACCOUNT_SHORT_NAME__C, "
              + "PAYMENT_TERMS__C, ACTIVE__C, ORACLE_CUSTOMER_ID__C, DISTI__C, "
              + "APTTUS_CONFIG2__BILLINGCONTACT, "
              + "IHV__C, LCM__C, ODM__C, OEM__C, VAR__C, APTTUS_CONFIG2__BILLINGDAYOFMO, "
              + "APTTUS_CONFIG2__BILLINGPREFERE, APTTUS_CONFIG2__CALENDARCYCLES, "
              + "APTTUS_CONFIG2__CERTIFICATEID_, "
              + "APTTUS_CONFIG2__CREDITMEMOEMAI, APTTUS_CONFIG2__DEFAULTCREDITM, "
              + "APTTUS_CONFIG2__DEFAULTINVOICE, APTTUS_CONFIG2__INVOICEEMAILTE, "
              + "APTTUS_CONFIG2__PAYMENTTERMID_, "
              + "APTTUS_CONFIG2__TAXEXEMPTSTATU, APTTUS_CONFIG2__TAXEXEMPT__C, "
              + "BILLINGCON_FIL_CRI, DEFAULTINVOICE_STMT_TEMP from ACCOUNT2 ");
      resultSet1 = preparedStatement2.executeQuery();
      while (resultSet1.next()) {
        String ID = resultSet1.getString("ID");
        String ISDELETED = resultSet1.getString("ISDELETED");
        String MASTERRECORDID = resultSet1.getString("MASTERRECORDID");
        String NAME = resultSet1.getString("NAME");
        String RECORDTYPEID = resultSet1.getString("RECORDTYPEID");
        String PARENTID = resultSet1.getString("PARENTID");
        String BILLINGSTREET = resultSet1.getString("BILLINGSTREET");
        String BILLINGCITY = resultSet1.getString("BILLINGCITY");
        String BILLINGSTATE = resultSet1.getString("BILLINGSTATE");
        String BILLINGPOSTALCODE = resultSet1.getString("BILLINGPOSTALCODE");
        String BILLINGCOUNTRY = resultSet1.getString("BILLINGCOUNTRY");
        String BILLINGLATITUDE = resultSet1.getString("BILLINGLATITUDE");
        String BILLINGLONGITUDE = resultSet1.getString("BILLINGLONGITUDE");
        String BILLINGGEOCODEACCURACY = resultSet1.getString("BILLINGGEOCODEACCURACY");
        String BILLINGADDRESS = resultSet1.getString("BILLINGADDRESS");
        String SHIPPINGSTREET = resultSet1.getString("SHIPPINGSTREET");
        String SHIPPINGCITY = resultSet1.getString("SHIPPINGCITY");
        String SHIPPINGSTATE = resultSet1.getString("SHIPPINGSTATE");
        String SHIPPINGPOSTALCODE = resultSet1.getString("SHIPPINGPOSTALCODE");
        String SHIPPINGCOUNTRY = resultSet1.getString("SHIPPINGCOUNTRY");
        String SHIPPINGLATITUDE = resultSet1.getString("SHIPPINGLATITUDE");
        String SHIPPINGLONGITUDE = resultSet1.getString("SHIPPINGLONGITUDE");
        String SHIPPINGGEOCODEACCURACY = resultSet1.getString("SHIPPINGGEOCODEACCURACY");
        String SHIPPINGADDRESS = resultSet1.getString("SHIPPINGADDRESS");
        String PHONE = resultSet1.getString("PHONE");
        String FAX = resultSet1.getString("FAX");
        String ACCOUNTNUMBER = resultSet1.getString("ACCOUNTNUMBER");
        String WEBSITE = resultSet1.getString("WEBSITE");
        String PHOTOURL = resultSet1.getString("PHOTOURL");
        String INDUSTRY = resultSet1.getString("INDUSTRY");
        String ANNUALREVENUE = resultSet1.getString("ANNUALREVENUE");
        String NUMBEROFEMPLOYEES = resultSet1.getString("NUMBEROFEMPLOYEES");
        String DESCRIPTION = resultSet1.getString("DESCRIPTION");
        String SITE = resultSet1.getString("SITE");
        String OWNERID = resultSet1.getString("OWNERID");
        String CREATEDDATE = resultSet1.getString("CREATEDDATE");
        String CREATEDBYID = resultSet1.getString("CREATEDBYID");
        String LASTMODIFIEDDATE = resultSet1.getString("LASTMODIFIEDDATE");
        String LASTMODIFIEDBYID = resultSet1.getString("LASTMODIFIEDBYID");
        String SYSTEMMODSTAMP = resultSet1.getString("SYSTEMMODSTAMP");
        String LASTACTIVITYDATE = resultSet1.getString("LASTACTIVITYDATE");
        String LASTVIEWEDDATE = resultSet1.getString("LASTVIEWEDDATE");
        String LASTREFERENCEDDATE = resultSet1.getString("LASTREFERENCEDDATE");
        String JIGSAW = resultSet1.getString("JIGSAW");
        String JIGSAWCOMPANYID = resultSet1.getString("JIGSAWCOMPANYID");
        String ACCOUNTSOURCE = resultSet1.getString("ACCOUNTSOURCE");
        String SICDESC = resultSet1.getString("SICDESC");
        String REGION__C = resultSet1.getString("REGION__C");
        String ACCOUNT_UNIQUE_KEY__C = resultSet1.getString("ACCOUNT_UNIQUE_KEY__C");
        String PARENT_ACCOUNT_NUMBER__C = resultSet1.getString("PARENT_ACCOUNT_NUMBER__C");
        String ACCOUNT_SHORT_NAME__C = resultSet1.getString("ACCOUNT_SHORT_NAME__C");
        String PAYMENT_TERMS__C = resultSet1.getString("PAYMENT_TERMS__C");
        String ACTIVE__C = resultSet1.getString("ACTIVE__C");
        Double ORACLE_CUSTOMER_ID__C = handleDouble(resultSet1.getString("ORACLE_CUSTOMER_ID__C"));
        String DISTI__C = resultSet1.getString("DISTI__C");
        String APTTUS_CONFIG2__BILLINGCONTACT =
            resultSet1.getString("APTTUS_CONFIG2__BILLINGCONTACT");
        String IHV__C = resultSet1.getString("IHV__C");
        String LCM__C = resultSet1.getString("LCM__C");
        String ODM__C = resultSet1.getString("ODM__C");
        String OEM__C = resultSet1.getString("OEM__C");
        String VAR__C = resultSet1.getString("VAR__C");
        String APTTUS_CONFIG2__BILLINGDAYOFMO =
            resultSet1.getString("APTTUS_CONFIG2__BILLINGDAYOFMO");
        String APTTUS_CONFIG2__BILLINGPREFERE =
            resultSet1.getString("APTTUS_CONFIG2__BILLINGPREFERE");
        String APTTUS_CONFIG2__CALENDARCYCLES =
            resultSet1.getString("APTTUS_CONFIG2__CALENDARCYCLES");
        String APTTUS_CONFIG2__CERTIFICATEID_ =
            resultSet1.getString("APTTUS_CONFIG2__CERTIFICATEID_");
        String APTTUS_CONFIG2__CREDITMEMOEMAI =
            resultSet1.getString("APTTUS_CONFIG2__CREDITMEMOEMAI");
        String APTTUS_CONFIG2__DEFAULTCREDITM =
            resultSet1.getString("APTTUS_CONFIG2__DEFAULTCREDITM");
        String APTTUS_CONFIG2__DEFAULTINVOICE =
            resultSet1.getString("APTTUS_CONFIG2__DEFAULTINVOICE");
        String APTTUS_CONFIG2__INVOICEEMAILTE =
            resultSet1.getString("APTTUS_CONFIG2__INVOICEEMAILTE");
        String APTTUS_CONFIG2__PAYMENTTERMID_ =
            resultSet1.getString("APTTUS_CONFIG2__PAYMENTTERMID_");
        String APTTUS_CONFIG2__TAXEXEMPTSTATU =
            resultSet1.getString("APTTUS_CONFIG2__TAXEXEMPTSTATU");
        String APTTUS_CONFIG2__TAXEXEMPT__C = resultSet1.getString("APTTUS_CONFIG2__TAXEXEMPT__C");
        String BILLINGCON_FIL_CRI = resultSet1.getString("BILLINGCON_FIL_CRI");
        String DEFAULTINVOICE_STMT_TEMP = resultSet1.getString("DEFAULTINVOICE_STMT_TEMP");
        insertAccount(ID, ISDELETED, MASTERRECORDID, NAME, RECORDTYPEID, PARENTID, BILLINGSTREET,
            BILLINGCITY, BILLINGSTATE, BILLINGPOSTALCODE, BILLINGCOUNTRY, BILLINGLATITUDE,
            BILLINGLONGITUDE, BILLINGGEOCODEACCURACY, BILLINGADDRESS, SHIPPINGSTREET, SHIPPINGCITY,
            SHIPPINGSTATE, SHIPPINGPOSTALCODE, SHIPPINGCOUNTRY, SHIPPINGLATITUDE, SHIPPINGLONGITUDE,
            SHIPPINGGEOCODEACCURACY, SHIPPINGADDRESS, PHONE, FAX, ACCOUNTNUMBER, WEBSITE, PHOTOURL,
            INDUSTRY, ANNUALREVENUE, NUMBEROFEMPLOYEES, DESCRIPTION, SITE, OWNERID, CREATEDDATE,
            CREATEDBYID, LASTMODIFIEDDATE, LASTMODIFIEDBYID, SYSTEMMODSTAMP, LASTACTIVITYDATE,
            LASTVIEWEDDATE, LASTREFERENCEDDATE, JIGSAW, JIGSAWCOMPANYID, ACCOUNTSOURCE, SICDESC,
            REGION__C, ACCOUNT_UNIQUE_KEY__C, PARENT_ACCOUNT_NUMBER__C, ACCOUNT_SHORT_NAME__C,
            PAYMENT_TERMS__C, ACTIVE__C, ORACLE_CUSTOMER_ID__C, DISTI__C,
            APTTUS_CONFIG2__BILLINGCONTACT, IHV__C, LCM__C, ODM__C, OEM__C, VAR__C,
            APTTUS_CONFIG2__BILLINGDAYOFMO, APTTUS_CONFIG2__BILLINGPREFERE,
            APTTUS_CONFIG2__CALENDARCYCLES, APTTUS_CONFIG2__CERTIFICATEID_,
            APTTUS_CONFIG2__CREDITMEMOEMAI, APTTUS_CONFIG2__DEFAULTCREDITM,
            APTTUS_CONFIG2__DEFAULTINVOICE, APTTUS_CONFIG2__INVOICEEMAILTE,
            APTTUS_CONFIG2__PAYMENTTERMID_, APTTUS_CONFIG2__TAXEXEMPTSTATU,
            APTTUS_CONFIG2__TAXEXEMPT__C, BILLINGCON_FIL_CRI, DEFAULTINVOICE_STMT_TEMP);
        System.out.println("Inserting into Account Id: " + ID);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
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
      if (connection2 != null) {
        try {
          connection2.close();
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
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void insertAccount(String ID, String ISDELETED, String MASTERRECORDID, String NAME,
      String RECORDTYPEID, String PARENTID, String BILLINGSTREET, String BILLINGCITY,
      String BILLINGSTATE, String BILLINGPOSTALCODE, String BILLINGCOUNTRY, String BILLINGLATITUDE,
      String BILLINGLONGITUDE, String BILLINGGEOCODEACCURACY, String BILLINGADDRESS,
      String SHIPPINGSTREET, String SHIPPINGCITY, String SHIPPINGSTATE, String SHIPPINGPOSTALCODE,
      String SHIPPINGCOUNTRY, String SHIPPINGLATITUDE, String SHIPPINGLONGITUDE,
      String SHIPPINGGEOCODEACCURACY, String SHIPPINGADDRESS, String PHONE, String FAX,
      String ACCOUNTNUMBER, String WEBSITE, String PHOTOURL, String INDUSTRY, String ANNUALREVENUE,
      String NUMBEROFEMPLOYEES, String DESCRIPTION, String SITE, String OWNERID, String CREATEDDATE,
      String CREATEDBYID, String LASTMODIFIEDDATE, String LASTMODIFIEDBYID, String SYSTEMMODSTAMP,
      String LASTACTIVITYDATE, String LASTVIEWEDDATE, String LASTREFERENCEDDATE, String JIGSAW,
      String JIGSAWCOMPANYID, String ACCOUNTSOURCE, String SICDESC, String REGION__C,
      String ACCOUNT_UNIQUE_KEY__C, String PARENT_ACCOUNT_NUMBER__C, String ACCOUNT_SHORT_NAME__C,
      String PAYMENT_TERMS__C, String ACTIVE__C, Double ORACLE_CUSTOMER_ID__C, String DISTI__C,
      String APTTUS_CONFIG2__BILLINGCONTACT, String IHV__C, String LCM__C, String ODM__C,
      String OEM__C, String VAR__C, String APTTUS_CONFIG2__BILLINGDAYOFMO,
      String APTTUS_CONFIG2__BILLINGPREFERE, String APTTUS_CONFIG2__CALENDARCYCLES,
      String APTTUS_CONFIG2__CERTIFICATEID_, String APTTUS_CONFIG2__CREDITMEMOEMAI,
      String APTTUS_CONFIG2__DEFAULTCREDITM, String APTTUS_CONFIG2__DEFAULTINVOICE,
      String APTTUS_CONFIG2__INVOICEEMAILTE, String APTTUS_CONFIG2__PAYMENTTERMID_,
      String APTTUS_CONFIG2__TAXEXEMPTSTATU, String APTTUS_CONFIG2__TAXEXEMPT__C,
      String BILLINGCON_FIL_CRI, String DEFAULTINVOICE_STMT_TEMP) throws IOException {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 =
          connection1.prepareStatement("INSERT INTO ACCOUNT(ID, ISDELETED, MASTERRECORDID, NAME, "
              + "RECORDTYPEID, PARENTID, BILLINGSTREET, BILLINGCITY, "
              + "BILLINGSTATE, BILLINGPOSTALCODE, BILLINGCOUNTRY, BILLINGLATITUDE, "
              + "BILLINGLONGITUDE, BILLINGGEOCODEACCURACY, BILLINGADDRESS, "
              + "SHIPPINGSTREET, SHIPPINGCITY, SHIPPINGSTATE, "
              + "SHIPPINGPOSTALCODE, SHIPPINGCOUNTRY, SHIPPINGLATITUDE, "
              + "SHIPPINGLONGITUDE, SHIPPINGGEOCODEACCURACY, SHIPPINGADDRESS, "
              + "PHONE, FAX, ACCOUNTNUMBER, WEBSITE, PHOTOURL, INDUSTRY, "
              + "ANNUALREVENUE, NUMBEROFEMPLOYEES, DESCRIPTION, SITE, OWNERID, "
              + "CREATEDDATE, CREATEDBYID, LASTMODIFIEDDATE, LASTMODIFIEDBYID, "
              + "SYSTEMMODSTAMP, LASTACTIVITYDATE, LASTVIEWEDDATE, LASTREFERENCEDDATE, "
              + "JIGSAW, JIGSAWCOMPANYID, ACCOUNTSOURCE, SICDESC, REGION__C, "
              + "ACCOUNT_UNIQUE_KEY__C, PARENT_ACCOUNT_NUMBER__C, ACCOUNT_SHORT_NAME__C, "
              + "PAYMENT_TERMS__C, ACTIVE__C, ORACLE_CUSTOMER_ID__C, DISTI__C, "
              + "APTTUS_CONFIG2__BILLINGCONTACT, IHV__C, LCM__C, ODM__C, OEM__C, VAR__C, "
              + "APTTUS_CONFIG2__BILLINGDAYOFMO, APTTUS_CONFIG2__BILLINGPREFERE, "
              + "APTTUS_CONFIG2__CALENDARCYCLES, APTTUS_CONFIG2__CERTIFICATEID_, "
              + "APTTUS_CONFIG2__CREDITMEMOEMAI, APTTUS_CONFIG2__DEFAULTCREDITM, "
              + "APTTUS_CONFIG2__DEFAULTINVOICE, APTTUS_CONFIG2__INVOICEEMAILTE, "
              + "APTTUS_CONFIG2__PAYMENTTERMID_, APTTUS_CONFIG2__TAXEXEMPTSTATU, "
              + "APTTUS_CONFIG2__TAXEXEMPT__C, BILLINGCON_FIL_CRI, DEFAULTINVOICE_STMT_TEMP)"
              + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?," + " ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
              + " ?, ?, ?, ?, ?, ?, ?, ?," + " ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      preparedStatement1.setString(1, ID);
      preparedStatement1.setString(2, ISDELETED);
      preparedStatement1.setString(3, MASTERRECORDID);
      preparedStatement1.setString(4, NAME);
      preparedStatement1.setString(5, RECORDTYPEID);
      preparedStatement1.setString(6, PARENTID);
      preparedStatement1.setString(7, BILLINGSTREET);
      preparedStatement1.setString(8, BILLINGCITY);
      preparedStatement1.setString(9, BILLINGSTATE);
      preparedStatement1.setString(10, BILLINGPOSTALCODE);
      preparedStatement1.setString(11, BILLINGCOUNTRY);
      preparedStatement1.setString(12, BILLINGLATITUDE);
      preparedStatement1.setString(13, BILLINGLONGITUDE);
      preparedStatement1.setString(14, BILLINGGEOCODEACCURACY);
      preparedStatement1.setString(15, BILLINGADDRESS);
      preparedStatement1.setString(16, SHIPPINGSTREET);
      preparedStatement1.setString(17, SHIPPINGCITY);
      preparedStatement1.setString(18, SHIPPINGSTATE);
      preparedStatement1.setString(19, SHIPPINGPOSTALCODE);
      preparedStatement1.setString(20, SHIPPINGCOUNTRY);
      preparedStatement1.setString(21, SHIPPINGLATITUDE);
      preparedStatement1.setString(22, SHIPPINGLONGITUDE);
      preparedStatement1.setString(23, SHIPPINGGEOCODEACCURACY);
      preparedStatement1.setString(24, SHIPPINGADDRESS);
      preparedStatement1.setString(25, PHONE);
      preparedStatement1.setString(26, FAX);
      preparedStatement1.setString(27, ACCOUNTNUMBER);
      preparedStatement1.setString(28, WEBSITE);
      preparedStatement1.setString(29, PHOTOURL);
      preparedStatement1.setString(30, INDUSTRY);
      preparedStatement1.setString(31, ANNUALREVENUE);
      preparedStatement1.setString(32, NUMBEROFEMPLOYEES);
      preparedStatement1.setString(33, DESCRIPTION);
      preparedStatement1.setString(34, SITE);
      preparedStatement1.setString(35, OWNERID);
      preparedStatement1.setString(36, CREATEDDATE);
      preparedStatement1.setString(37, CREATEDBYID);
      preparedStatement1.setString(38, LASTMODIFIEDDATE);
      preparedStatement1.setString(39, LASTMODIFIEDBYID);
      preparedStatement1.setString(40, SYSTEMMODSTAMP);
      preparedStatement1.setString(41, LASTACTIVITYDATE);
      preparedStatement1.setString(42, LASTVIEWEDDATE);
      preparedStatement1.setString(43, LASTREFERENCEDDATE);
      preparedStatement1.setString(44, JIGSAW);
      preparedStatement1.setString(45, JIGSAWCOMPANYID);
      preparedStatement1.setString(46, ACCOUNTSOURCE);
      preparedStatement1.setString(47, SICDESC);
      preparedStatement1.setString(48, REGION__C);
      preparedStatement1.setString(49, ACCOUNT_UNIQUE_KEY__C);
      preparedStatement1.setString(50, PARENT_ACCOUNT_NUMBER__C);
      preparedStatement1.setString(51, ACCOUNT_SHORT_NAME__C);
      preparedStatement1.setString(52, PAYMENT_TERMS__C);
      preparedStatement1.setString(53, ACTIVE__C);
      preparedStatement1.setDouble(54, ORACLE_CUSTOMER_ID__C);
      preparedStatement1.setString(55, DISTI__C);
      preparedStatement1.setString(56, APTTUS_CONFIG2__BILLINGCONTACT);
      preparedStatement1.setString(57, IHV__C);
      preparedStatement1.setString(58, LCM__C);
      preparedStatement1.setString(59, ODM__C);
      preparedStatement1.setString(60, OEM__C);
      preparedStatement1.setString(61, VAR__C);
      preparedStatement1.setString(62, APTTUS_CONFIG2__BILLINGDAYOFMO);
      preparedStatement1.setString(63, APTTUS_CONFIG2__BILLINGPREFERE);
      preparedStatement1.setString(64, APTTUS_CONFIG2__CALENDARCYCLES);
      preparedStatement1.setString(65, APTTUS_CONFIG2__CERTIFICATEID_);
      preparedStatement1.setString(66, APTTUS_CONFIG2__CREDITMEMOEMAI);
      preparedStatement1.setString(67, APTTUS_CONFIG2__DEFAULTCREDITM);
      preparedStatement1.setString(68, APTTUS_CONFIG2__DEFAULTINVOICE);
      preparedStatement1.setString(69, APTTUS_CONFIG2__INVOICEEMAILTE);
      preparedStatement1.setString(70, APTTUS_CONFIG2__PAYMENTTERMID_);
      preparedStatement1.setString(71, APTTUS_CONFIG2__TAXEXEMPTSTATU);
      preparedStatement1.setString(72, APTTUS_CONFIG2__TAXEXEMPT__C);
      preparedStatement1.setString(73, BILLINGCON_FIL_CRI);
      preparedStatement1.setString(74, DEFAULTINVOICE_STMT_TEMP);
      preparedStatement1.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void queryProduct2() {
    Connection connection1 = null;
    Connection connection2 = null;
    PreparedStatement preparedStatement1 = null;
    PreparedStatement preparedStatement2 = null;
    ResultSet resultSet1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1.prepareStatement("Truncate Table PRODUCT2");
      preparedStatement1.executeUpdate();
      connection2 = getConnection();
      preparedStatement2 = connection2
          .prepareStatement("select ID, NAME, PRODUCTCODE, DESCRIPTION, QUANTITYSCHEDULETYPE, "
              + "QUANTITYINSTALLMENTPERIOD, "
              + "NUMBEROFQUANTITYINSTALLMENTS, REVENUESCHEDULETYPE, "
              + "REVENUEINSTALLMENTPERIOD, NUMBEROFREVENUEINSTALLMENTS, CANUSEQUANTITYSCHEDULE, "
              + "CANUSEREVENUESCHEDULE, ISACTIVE, CREATEDDATE, CREATEDBYID, "
              + "LASTMODIFIEDDATE, LASTMODIFIEDBYID, SYSTEMMODSTAMP, RECORDTYPEID, ISDELETED, "
              + "LASTVIEWEDDATE, LASTREFERENCEDDATE, DIVISION__C, "
              + "PRODUCT_TYPE__C, PRODUCT_GROUP__C, PRODUCT_SERIES__C, "
              + "APTTUS_CONFIG2__BUNDLEINVOICEL, "
              + "APTTUS_CONFIG2__CONFIGURATIONT, APTTUS_CONFIG2__CUSTOMIZABLE__, "
              + "APTTUS_CONFIG2__DISCONTINUEDDA, APTTUS_CONFIG2__EFFECTIVEDATE_, "
              + "APTTUS_CONFIG2__EFFECTIVESTART, APTTUS_CONFIG2__EXCLUDEFROMSIT, "
              + "APTTUS_CONFIG2__EXPIRATIONDATE, APTTUS_CONFIG2__HASATTRIBUTES_, "
              + "APTTUS_CONFIG2__HASDEFAULTS__C, APTTUS_CONFIG2__HASOPTIONS__C, "
              + "APTTUS_CONFIG2__HASSEARCHATTRI, APTTUS_CONFIG2__ICONID__C, "
              + "APTTUS_CONFIG2__ICONSIZE__C, "
              + "APTTUS_CONFIG2__ICON__C, APTTUS_CONFIG2__LAUNCHDATE__C, "
              + "APTTUS_CONFIG2__UOM__C, APTTUS_CONFIG2__VERSION__C, "
              + "APTS_COST__C, AGILE_PART_ID__C, LIFECYCLE__C, APTS_IS_DUMMY__C, SYN_PART_TYPE__C, "
              + "PRODUCT_FAMILY_SEGMENT_CODE__C, SYN_EXCLUDE_FROM_PRICING_PRODU FROM PRODUCT22");
      resultSet1 = preparedStatement2.executeQuery();
      while (resultSet1.next()) {
        String ID = resultSet1.getString("ID");
        String NAME = resultSet1.getString("NAME");
        String PRODUCTCODE = resultSet1.getString("PRODUCTCODE");
        String DESCRIPTION = resultSet1.getString("DESCRIPTION");
        String QUANTITYSCHEDULETYPE = resultSet1.getString("QUANTITYSCHEDULETYPE");
        String QUANTITYINSTALLMENTPERIOD = resultSet1.getString("QUANTITYINSTALLMENTPERIOD");
        String NUMBEROFQUANTITYINSTALLMENTS = resultSet1.getString("NUMBEROFQUANTITYINSTALLMENTS");
        String REVENUESCHEDULETYPE = resultSet1.getString("REVENUESCHEDULETYPE");
        String REVENUEINSTALLMENTPERIOD = resultSet1.getString("REVENUEINSTALLMENTPERIOD");
        String NUMBEROFREVENUEINSTALLMENTS = resultSet1.getString("NUMBEROFREVENUEINSTALLMENTS");
        String CANUSEQUANTITYSCHEDULE = resultSet1.getString("CANUSEQUANTITYSCHEDULE");
        String CANUSEREVENUESCHEDULE = resultSet1.getString("CANUSEREVENUESCHEDULE");
        String ISACTIVE = resultSet1.getString("ISACTIVE");
        Date CREATEDDATE = handleDate(resultSet1.getString("CREATEDDATE"));
        String CREATEDBYID = resultSet1.getString("CREATEDBYID");
        Date LASTMODIFIEDDATE = handleDate(resultSet1.getString("LASTMODIFIEDDATE"));
        String LASTMODIFIEDBYID = resultSet1.getString("LASTMODIFIEDBYID");
        Date SYSTEMMODSTAMP = handleDate(resultSet1.getString("SYSTEMMODSTAMP"));
        String RECORDTYPEID = resultSet1.getString("RECORDTYPEID");
        String ISDELETED = resultSet1.getString("ISDELETED");
        Date LASTVIEWEDDATE = handleDate(resultSet1.getString("LASTVIEWEDDATE"));
        Date LASTREFERENCEDDATE = handleDate(resultSet1.getString("LASTREFERENCEDDATE"));
        String DIVISION__C = resultSet1.getString("DIVISION__C");
        String PRODUCT_TYPE__C = resultSet1.getString("PRODUCT_TYPE__C");
        String PRODUCT_GROUP__C = resultSet1.getString("PRODUCT_GROUP__C");
        String PRODUCT_SERIES__C = resultSet1.getString("PRODUCT_SERIES__C");
        String APTTUS_CONFIG2__BUNDLEINVOICEL =
            resultSet1.getString("APTTUS_CONFIG2__BUNDLEINVOICEL");
        String APTTUS_CONFIG2__CONFIGURATIONT =
            resultSet1.getString("APTTUS_CONFIG2__CONFIGURATIONT");
        String APTTUS_CONFIG2__CUSTOMIZABLE__ =
            resultSet1.getString("APTTUS_CONFIG2__CUSTOMIZABLE__");
        Date APTTUS_CONFIG2__DISCONTINUEDDA =
            handleDate(resultSet1.getString("APTTUS_CONFIG2__DISCONTINUEDDA"));
        Date APTTUS_CONFIG2__EFFECTIVEDATE_ =
            handleDate(resultSet1.getString("APTTUS_CONFIG2__EFFECTIVEDATE_"));
        Date APTTUS_CONFIG2__EFFECTIVESTART =
            handleDate(resultSet1.getString("APTTUS_CONFIG2__EFFECTIVESTART"));
        String APTTUS_CONFIG2__EXCLUDEFROMSIT =
            resultSet1.getString("APTTUS_CONFIG2__EXCLUDEFROMSIT");
        Date APTTUS_CONFIG2__EXPIRATIONDATE =
            handleDate(resultSet1.getString("APTTUS_CONFIG2__EXPIRATIONDATE"));
        String APTTUS_CONFIG2__HASATTRIBUTES_ =
            resultSet1.getString("APTTUS_CONFIG2__HASATTRIBUTES_");
        String APTTUS_CONFIG2__HASDEFAULTS__C =
            resultSet1.getString("APTTUS_CONFIG2__HASDEFAULTS__C");
        String APTTUS_CONFIG2__HASOPTIONS__C =
            resultSet1.getString("APTTUS_CONFIG2__HASOPTIONS__C");
        String APTTUS_CONFIG2__HASSEARCHATTRI =
            resultSet1.getString("APTTUS_CONFIG2__HASSEARCHATTRI");
        String APTTUS_CONFIG2__ICONID__C = resultSet1.getString("APTTUS_CONFIG2__ICONID__C");
        String APTTUS_CONFIG2__ICONSIZE__C = resultSet1.getString("APTTUS_CONFIG2__ICONSIZE__C");
        String APTTUS_CONFIG2__ICON__C = null;
        Date APTTUS_CONFIG2__LAUNCHDATE__C =
            handleDate(resultSet1.getString("APTTUS_CONFIG2__LAUNCHDATE__C"));
        String APTTUS_CONFIG2__UOM__C = resultSet1.getString("APTTUS_CONFIG2__UOM__C");
        Double APTTUS_CONFIG2__VERSION__C =
            handleDouble(resultSet1.getString("APTTUS_CONFIG2__VERSION__C"));
        Double APTS_COST__C = handleDouble(resultSet1.getString("APTS_COST__C"));
        String AGILE_PART_ID__C = resultSet1.getString("AGILE_PART_ID__C");
        String LIFECYCLE__C = resultSet1.getString("LIFECYCLE__C");
        String APTS_IS_DUMMY__C = resultSet1.getString("APTS_IS_DUMMY__C");
        String SYN_PART_TYPE__C = resultSet1.getString("SYN_PART_TYPE__C");
        String PRODUCT_FAMILY_SEGMENT_CODE__C =
            resultSet1.getString("PRODUCT_FAMILY_SEGMENT_CODE__C");
        String SYN_EXCLUDE_FROM_PRICING_PRODU =
            resultSet1.getString("SYN_EXCLUDE_FROM_PRICING_PRODU");
        insertProduct2(ID, NAME, PRODUCTCODE, DESCRIPTION, QUANTITYSCHEDULETYPE,
            QUANTITYINSTALLMENTPERIOD, NUMBEROFQUANTITYINSTALLMENTS, REVENUESCHEDULETYPE,
            REVENUEINSTALLMENTPERIOD, NUMBEROFREVENUEINSTALLMENTS, CANUSEQUANTITYSCHEDULE,
            CANUSEREVENUESCHEDULE, ISACTIVE, CREATEDDATE, CREATEDBYID, LASTMODIFIEDDATE,
            LASTMODIFIEDBYID, SYSTEMMODSTAMP, RECORDTYPEID, ISDELETED, LASTVIEWEDDATE,
            LASTREFERENCEDDATE, DIVISION__C, PRODUCT_TYPE__C, PRODUCT_GROUP__C, PRODUCT_SERIES__C,
            APTTUS_CONFIG2__BUNDLEINVOICEL, APTTUS_CONFIG2__CONFIGURATIONT,
            APTTUS_CONFIG2__CUSTOMIZABLE__, APTTUS_CONFIG2__DISCONTINUEDDA,
            APTTUS_CONFIG2__EFFECTIVEDATE_, APTTUS_CONFIG2__EFFECTIVESTART,
            APTTUS_CONFIG2__EXCLUDEFROMSIT, APTTUS_CONFIG2__EXPIRATIONDATE,
            APTTUS_CONFIG2__HASATTRIBUTES_, APTTUS_CONFIG2__HASDEFAULTS__C,
            APTTUS_CONFIG2__HASOPTIONS__C, APTTUS_CONFIG2__HASSEARCHATTRI,
            APTTUS_CONFIG2__ICONID__C, APTTUS_CONFIG2__ICONSIZE__C, APTTUS_CONFIG2__ICON__C,
            APTTUS_CONFIG2__LAUNCHDATE__C, APTTUS_CONFIG2__UOM__C, APTTUS_CONFIG2__VERSION__C,
            APTS_COST__C, AGILE_PART_ID__C, LIFECYCLE__C, APTS_IS_DUMMY__C, SYN_PART_TYPE__C,
            PRODUCT_FAMILY_SEGMENT_CODE__C, SYN_EXCLUDE_FROM_PRICING_PRODU);
        System.out.println("Inserting into Product2 Id: " + ID);
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
      if (connection2 != null) {
        try {
          connection2.close();
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
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void insertProduct2(String ID, String NAME, String PRODUCTCODE, String DESCRIPTION,
      String QUANTITYSCHEDULETYPE, String QUANTITYINSTALLMENTPERIOD,
      String NUMBEROFQUANTITYINSTALLMENTS, String REVENUESCHEDULETYPE,
      String REVENUEINSTALLMENTPERIOD, String NUMBEROFREVENUEINSTALLMENTS,
      String CANUSEQUANTITYSCHEDULE, String CANUSEREVENUESCHEDULE, String ISACTIVE,
      Date CREATEDDATE, String CREATEDBYID, Date LASTMODIFIEDDATE, String LASTMODIFIEDBYID,
      Date SYSTEMMODSTAMP, String RECORDTYPEID, String ISDELETED, Date LASTVIEWEDDATE,
      Date LASTREFERENCEDDATE, String DIVISION__C, String PRODUCT_TYPE__C, String PRODUCT_GROUP__C,
      String PRODUCT_SERIES__C, String APTTUS_CONFIG2__BUNDLEINVOICEL,
      String APTTUS_CONFIG2__CONFIGURATIONT, String APTTUS_CONFIG2__CUSTOMIZABLE__,
      Date APTTUS_CONFIG2__DISCONTINUEDDA, Date APTTUS_CONFIG2__EFFECTIVEDATE_,
      Date APTTUS_CONFIG2__EFFECTIVESTART, String APTTUS_CONFIG2__EXCLUDEFROMSIT,
      Date APTTUS_CONFIG2__EXPIRATIONDATE, String APTTUS_CONFIG2__HASATTRIBUTES_,
      String APTTUS_CONFIG2__HASDEFAULTS__C, String APTTUS_CONFIG2__HASOPTIONS__C,
      String APTTUS_CONFIG2__HASSEARCHATTRI, String APTTUS_CONFIG2__ICONID__C,
      String APTTUS_CONFIG2__ICONSIZE__C, String APTTUS_CONFIG2__ICON__C,
      Date APTTUS_CONFIG2__LAUNCHDATE__C, String APTTUS_CONFIG2__UOM__C,
      Double APTTUS_CONFIG2__VERSION__C, Double APTS_COST__C, String AGILE_PART_ID__C,
      String LIFECYCLE__C, String APTS_IS_DUMMY__C, String SYN_PART_TYPE__C,
      String PRODUCT_FAMILY_SEGMENT_CODE__C, String SYN_EXCLUDE_FROM_PRICING_PRODU) {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1.prepareStatement(
          "INSERT INTO PRODUCT2(ID, NAME, PRODUCTCODE, DESCRIPTION, " + "QUANTITYSCHEDULETYPE, "
              + "QUANTITYINSTALLMENTPERIOD, NUMBEROFQUANTITYINSTALLMENTS, REVENUESCHEDULETYPE, "
              + "REVENUEINSTALLMENTPERIOD, NUMBEROFREVENUEINSTALLMENTS, CANUSEQUANTITYSCHEDULE, "
              + "CANUSEREVENUESCHEDULE, ISACTIVE, CREATEDDATE, CREATEDBYID, LASTMODIFIEDDATE, "
              + "LASTMODIFIEDBYID, SYSTEMMODSTAMP, RECORDTYPEID, ISDELETED, LASTVIEWEDDATE, "
              + "LASTREFERENCEDDATE, DIVISION__C, PRODUCT_TYPE__C, PRODUCT_GROUP__C, "
              + "PRODUCT_SERIES__C, APTTUS_CONFIG2__BUNDLEINVOICEL, APTTUS_CONFIG2__CONFIGURATIONT,"
              + "APTTUS_CONFIG2__CUSTOMIZABLE__, APTTUS_CONFIG2__DISCONTINUEDDA, "
              + "APTTUS_CONFIG2__EFFECTIVEDATE_, APTTUS_CONFIG2__EFFECTIVESTART, "
              + "APTTUS_CONFIG2__EXCLUDEFROMSIT, APTTUS_CONFIG2__EXPIRATIONDATE, "
              + "APTTUS_CONFIG2__HASATTRIBUTES_, APTTUS_CONFIG2__HASDEFAULTS__C, "
              + "APTTUS_CONFIG2__HASOPTIONS__C, APTTUS_CONFIG2__HASSEARCHATTRI, "
              + "APTTUS_CONFIG2__ICONID__C, APTTUS_CONFIG2__ICONSIZE__C, "
              + "APTTUS_CONFIG2__ICON__C, APTTUS_CONFIG2__LAUNCHDATE__C, "
              + "APTTUS_CONFIG2__UOM__C, APTTUS_CONFIG2__VERSION__C, APTS_COST__C, "
              + "AGILE_PART_ID__C, LIFECYCLE__C, APTS_IS_DUMMY__C, SYN_PART_TYPE__C, "
              + "PRODUCT_FAMILY_SEGMENT_CODE__C, SYN_EXCLUDE_FROM_PRICING_PRODU)"
              + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, " + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      preparedStatement1.setString(1, ID);
      preparedStatement1.setString(2, NAME);
      preparedStatement1.setString(3, PRODUCTCODE);
      preparedStatement1.setString(4, DESCRIPTION);
      preparedStatement1.setString(5, QUANTITYSCHEDULETYPE);
      preparedStatement1.setString(6, QUANTITYINSTALLMENTPERIOD);
      preparedStatement1.setString(7, NUMBEROFQUANTITYINSTALLMENTS);
      preparedStatement1.setString(8, REVENUESCHEDULETYPE);
      preparedStatement1.setString(9, REVENUEINSTALLMENTPERIOD);
      preparedStatement1.setString(10, NUMBEROFREVENUEINSTALLMENTS);
      preparedStatement1.setString(11, CANUSEQUANTITYSCHEDULE);
      preparedStatement1.setString(12, CANUSEREVENUESCHEDULE);
      preparedStatement1.setString(13, ISACTIVE);
      preparedStatement1.setDate(14, CREATEDDATE);
      preparedStatement1.setString(15, CREATEDBYID);
      preparedStatement1.setDate(16, LASTMODIFIEDDATE);
      preparedStatement1.setString(17, LASTMODIFIEDBYID);
      preparedStatement1.setDate(18, SYSTEMMODSTAMP);
      preparedStatement1.setString(19, RECORDTYPEID);
      preparedStatement1.setString(20, ISDELETED);
      preparedStatement1.setDate(21, LASTVIEWEDDATE);
      preparedStatement1.setDate(22, LASTREFERENCEDDATE);
      preparedStatement1.setString(23, DIVISION__C);
      preparedStatement1.setString(24, PRODUCT_TYPE__C);
      preparedStatement1.setString(25, PRODUCT_GROUP__C);
      preparedStatement1.setString(26, PRODUCT_SERIES__C);
      preparedStatement1.setString(27, APTTUS_CONFIG2__BUNDLEINVOICEL);
      preparedStatement1.setString(28, APTTUS_CONFIG2__CONFIGURATIONT);
      preparedStatement1.setString(29, APTTUS_CONFIG2__CUSTOMIZABLE__);
      preparedStatement1.setDate(30, APTTUS_CONFIG2__DISCONTINUEDDA);
      preparedStatement1.setDate(31, APTTUS_CONFIG2__EFFECTIVEDATE_);
      preparedStatement1.setDate(32, APTTUS_CONFIG2__EFFECTIVESTART);
      preparedStatement1.setString(33, APTTUS_CONFIG2__EXCLUDEFROMSIT);
      preparedStatement1.setDate(34, APTTUS_CONFIG2__EXPIRATIONDATE);
      preparedStatement1.setString(35, APTTUS_CONFIG2__HASATTRIBUTES_);
      preparedStatement1.setString(36, APTTUS_CONFIG2__HASDEFAULTS__C);
      preparedStatement1.setString(37, APTTUS_CONFIG2__HASOPTIONS__C);
      preparedStatement1.setString(38, APTTUS_CONFIG2__HASSEARCHATTRI);
      preparedStatement1.setString(39, APTTUS_CONFIG2__ICONID__C);
      preparedStatement1.setString(40, APTTUS_CONFIG2__ICONSIZE__C);
      preparedStatement1.setString(41, APTTUS_CONFIG2__ICON__C);
      preparedStatement1.setDate(42, APTTUS_CONFIG2__LAUNCHDATE__C);
      preparedStatement1.setString(43, APTTUS_CONFIG2__UOM__C);
      preparedStatement1.setDouble(44, APTTUS_CONFIG2__VERSION__C);
      preparedStatement1.setDouble(45, APTS_COST__C);
      preparedStatement1.setString(46, AGILE_PART_ID__C);
      preparedStatement1.setString(47, LIFECYCLE__C);
      preparedStatement1.setString(48, APTS_IS_DUMMY__C);
      preparedStatement1.setString(49, SYN_PART_TYPE__C);
      preparedStatement1.setString(50, PRODUCT_FAMILY_SEGMENT_CODE__C);
      preparedStatement1.setString(51, SYN_EXCLUDE_FROM_PRICING_PRODU);
      preparedStatement1.executeUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void queryOpportunity() {
    Connection connection1 = null;
    Connection connection2 = null;
    PreparedStatement preparedStatement1 = null;
    PreparedStatement preparedStatement2 = null;
    ResultSet resultSet1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1.prepareStatement("Truncate Table OPPORTUNITY");
      preparedStatement1.executeUpdate();
      connection2 = getConnection();
      preparedStatement2 = connection2.prepareStatement("select ACCOUNTID, "
          + "ACTUALS_AMOUNT__C, ACTUAL_DESIGN_IN_DATE__C, ACTUAL_SYNA_MP_DATE__C, "
          + "AMOUNT, APPLICATION__C, APTS_ALLOW_OPPORTUNITY_STAGE_C, APTS_PROJECT_TYPE__C, "
          + "APTTUS_APPROVAL__APPROVAL_STAT, APTTUS_PROPOSAL__DISCOUNT_PERC, ASP__C, "
          + "AUDIT_ACTUAL_DW_VS_CLOSEDATE__, AVERAGE_MONTHLY_QUANTITY__C, "
          + "BUDGETARY_PRICE_APPROVAL_DATE_, BUDGETARY__C, BU_PRICE_APPROVED_DATE__C, "
          + "CAMPAIGNID, CHANNEL__C, CLEARPAD_SERIES__C, CLOSEDATE, CLOSED_DATE_PAST_DUE__C, "
          + "COUNT_OF_BU_REQUESTED_TIMESTAM, CREATEDBYID, CREATEDDATE, "
          + "CURRENT_SALES_NET_PRICE__C, CUSTOMER_ID__C, CUSTOMER_PART_NUMBER__C, "
          + "CUSTOMER_REQUIREMENTS__C, CUSTOMER_SPECIFIC_QUOTE_CONDIT, "
          + "DAYS_SINCE_LAST_MODIFIED_BY_US, DAYS_TO_CLOSE_DATE__C, "
          + "DAYS_TO_DESIGN_IN__C, DESCRIPTION, DESIGN_IN_DATE__C, "
          + "DISPLAY_SIZE_TYPE__C, DIVISION__C, EARLY_SAMPLE_DATE__C, ENGAGEMENT_MODEL__C, "
          + "ESTIMATED_REVENUE__C, EXPECTED_DESIGN_WIN_DATE__C, EXPECTED_RFI_RFQ_DATE__C, "
          + "FIRST_CUSTOMER_PO_NUMBER__C, FORECASTCATEGORY, FORECASTCATEGORYNAME, "
          + "FORECAST_QUOTE_PRIMARY_PRODUCT, FORECAST_QUOTE__C, FY16_QUOTE__C, "
          + "GESTURES_CAPABILITIES__C, GESTURE_SUPPORT__C, HALOGEN_FREE_SUPPORT__C, "
          + "HASOPENACTIVITY, HASOPPORTUNITYLINEITEM, HASOVERDUETASK, ID, INTERFACE__C, "
          + "ISCLOSED, ISDELETED, ISPRIVATE, ISWON, JIRA_NUMBER__C, LASTACTIVITYDATE, "
          + "LASTMODIFIEDBYID, LASTMODIFIEDDATE, LASTREFERENCEDDATE, LASTVIEWEDDATE, "
          + "LAST_UPDATED_BY_USER__C, LCM_OPPORTUNITY_LOOKUP__C, LCM_REFERENCE_ACCOUNT__C, "
          + "LCM_TO_OEM_REFERENCE_ACCOUNT__, LCM_TO_OEM_REFERENCE_OPPORTUNI, LEADSOURCE, "
          + "LIFETIME_QUANTITY_TIER__C, LIFETIME_QUANTITY__C, MARKET_SEGMENT_CODE__C, "
          + "MOBILE_OS__C, MODEL_NAME_IN_MARKET__C, MOQ__C, MP_DATE__C, "
          + "MP_FORECAST_FLAG__C, NAME, NEXTSTEP, NOTEBOOK_LCD_SIZE__C, "
          + "NUMBER_OF_COMPETITORS__C, NUMBER_OF_DAYS_PAST_DUE__C, "
          + "NUMBER_OF_MONTHS_LIFETIME__C, NUMBER_OF_OPPORTUNITIES__C, "
          + "NUMBER_OF_PRODUCTS__C, NUMBER_OF_QUOTES_WITH_PRODUCTS, NUMBER_OF_QUOTES__C, "
          + "ODM_PROGRAM_NAME__C, ODM_REFERENCE_ACCOUNT__C, ODM_SUGGESTED_FSF__C, "
          + "OEM_OPPORTUNITY_LOOKUP__C, OEM_OPPORTUNITY_NAME__C, OEM_PROGRAM_NAME_INPUT__C, "
          + "OEM_REFERENCE_ACCOUNT_ID__C, OEM_REFERENCE_ACCOUNT__C, OEM_SUGGESTED_FSF__C, "
          + "OEM_VAR_ACCOUNT__C, OF_ADDITIONAL_QUOTES__C, OF_RELATED_ODM_S__C, OID__C, "
          + "OMIT_FROM_FORECAST__C, OPA_SAME_ACCOUNT_ROLLUP__C, OPPORTUNITY_FORECAST__C, "
          + "OPPORTUNITY_LIFETIME_QUANTITY_, OPPORTUNITY_OWNER_EMP_ID__C, "
          + "OPPORTUNITY_REPORT_NAME__C, OPPORTUNITY_STAGE_SCORE__C, OPPORTUNITY_VOLUME__C, "
          + "ORACLE_SALES_ORDER_NUMBER__C, ORIGINAL_PART_NUMBER__C, OS__C, "
          + "OTHER_INTERFACE_DETAILS__C, OWNERID, PANEL_RESOLUTION__C, PANEL_SIZE__C, "
          + "PARENT_OEM_ACCOUNT__C, PHONE_TYPE__C, PLANNING_POLICY__C, PRECISION_TOUCHPAD__C, "
          + "PREFERRED_SUPPLIER_LENS__C, PREFERRED_SUPPLIER_SENSOR__C, PRICEBOOK2ID, "
          + "PRICE_REDUCTION_CONSOLE__C, PRIMARY_APPROVED__C, PRIMARY_CONTACT__C, "
          + "PRIMARY_PRODUCT_STATUS__C, PRIMARY_QUOTE_BILL_TO_NAME__C, PRIMARY_SOLUTION__C, "
          + "PROBABILITY, PRODUCT_GROUP__C, PRODUCT_TYPE2__C, PRODUCT__C, "
          + "PROJECT_ACCOUNT_NAME_S_REGION_, PROJECT_MODEL_NAME_IN_MARKET__, "
          + "PROJECT_VOLUME__C, PROJECT__C, PURCHASE_ORDER_DATE__C, QUANTITY_PER_MONTH__C, "
          + "QUOTE_BILL_TO_PAYMENT_TERMS__C, QUOTE_BILL_TO__C, QUOTE_CONDITIONS__C, "
          + "QUOTE_SENT_TO_CUSTOMER__C, QUOTE_STAGE_VALUE_ROLLUP__C, RECORDTYPEID, "
          + "REP__C, REQUEST_FOR_BUDGETARY_QUOTE__C, REQUEST_FOR_RE_QUOTE__C, "
          + "RE_QUOTE__C, RFQ__C, ROLLUPCOMPETITORS__C, ROLLUP_ACTUAL_DESIGN_WIN_DATE_, "
          + "RUNNING_USER__C, SALES_REGION__C, SEGMENTATION__C, SFID__C, SHIPPING_TERMS__C, "
          + "SINGLE_MULTI_SOURCE__C, SOW_SENT_TO_BU__C, STAGENAME, STANDARDIZED_OPPORTUNITY_NAME_, "
          + "STRATEGIC__C, SUBMIT_FOR_QUOTE__C, SYNAPTICS_SHARE_PERCENTAGE__C, "
          + "SYNA_PART_NUMBER__C, SYNA_SHARE__C, SYNCEDQUOTEID, SYSTEMMODSTAMP, "
          + "TOTAL_CUSTOMER_PROJECT_QTY__C, TOTAL_OPPORTUNITY_FAE_INVOLVEM, TYPE, "
          + "WHITEBOX__C, WIN_RATE__C, FORECAST_QU_PRI_PRO_NAME_C, STAND_OPPO_NAME_INDEXED_C"
          + "from OPPORTUNITY2");
      resultSet1 = preparedStatement2.executeQuery();
      while (resultSet1.next()) {
        String ACCOUNTID = resultSet1.getString("ACCOUNTID");
        Double ACTUALS_AMOUNT__C = handleDouble(resultSet1.getString("ACTUALS_AMOUNT__C"));
        Date ACTUAL_DESIGN_IN_DATE__C =
            handleDate(resultSet1.getString("ACTUAL_DESIGN_IN_DATE__C"));
        Date ACTUAL_SYNA_MP_DATE__C = handleDate(resultSet1.getString("ACTUAL_SYNA_MP_DATE__C"));
        Double AMOUNT = handleDouble(resultSet1.getString("AMOUNT"));
        String APPLICATION__C = resultSet1.getString("APPLICATION__C");
        String APTS_ALLOW_OPPORTUNITY_STAGE_C =
            resultSet1.getString("APTS_ALLOW_OPPORTUNITY_STAGE_C");
        String APTS_PROJECT_TYPE__C = resultSet1.getString("APTS_PROJECT_TYPE__C");
        String APTTUS_APPROVAL__APPROVAL_STAT =
            resultSet1.getString("APTTUS_APPROVAL__APPROVAL_STAT");
        Double APTTUS_PROPOSAL__DISCOUNT_PERC =
            handleDouble(resultSet1.getString("APTTUS_PROPOSAL__DISCOUNT_PERC"));
        Double ASP__C = handleDouble(resultSet1.getString("ASP__C"));
        String AUDIT_ACTUAL_DW_VS_CLOSEDATE__ =
            resultSet1.getString("AUDIT_ACTUAL_DW_VS_CLOSEDATE__");
        Double AVERAGE_MONTHLY_QUANTITY__C =
            handleDouble(resultSet1.getString("AVERAGE_MONTHLY_QUANTITY__C"));
        Timestamp BUDGETARY_PRICE_APPROVAL_DATE_ =
            handleTimestamp(resultSet1.getString("BUDGETARY_PRICE_APPROVAL_DATE_"));
        String BUDGETARY__C = resultSet1.getString("BUDGETARY__C");
        Timestamp BU_PRICE_APPROVED_DATE__C =
            handleTimestamp(resultSet1.getString("BU_PRICE_APPROVED_DATE__C"));
        String CAMPAIGNID = resultSet1.getString("CAMPAIGNID");
        String CHANNEL__C = resultSet1.getString("CHANNEL__C");
        String CLEARPAD_SERIES__C = resultSet1.getString("CLEARPAD_SERIES__C");
        Date CLOSEDATE = handleDate(resultSet1.getString("CLOSEDATE"));
        String CLOSED_DATE_PAST_DUE__C = resultSet1.getString("CLOSED_DATE_PAST_DUE__C");
        Double COUNT_OF_BU_REQUESTED_TIMESTAM =
            handleDouble(resultSet1.getString("COUNT_OF_BU_REQUESTED_TIMESTAM"));
        String CREATEDBYID = resultSet1.getString("CREATEDBYID");
        Timestamp CREATEDDATE = handleTimestamp(resultSet1.getString("CREATEDDATE"));
        Double CURRENT_SALES_NET_PRICE__C =
            handleDouble(resultSet1.getString("CURRENT_SALES_NET_PRICE__C"));
        String CUSTOMER_ID__C = resultSet1.getString("CUSTOMER_ID__C");
        String CUSTOMER_PART_NUMBER__C = resultSet1.getString("CUSTOMER_PART_NUMBER__C");
        String CUSTOMER_REQUIREMENTS__C = resultSet1.getString("CUSTOMER_REQUIREMENTS__C");
        String CUSTOMER_SPECIFIC_QUOTE_CONDIT =
            resultSet1.getString("CUSTOMER_SPECIFIC_QUOTE_CONDIT");
        Double DAYS_SINCE_LAST_MODIFIED_BY_US =
            handleDouble(resultSet1.getString("DAYS_SINCE_LAST_MODIFIED_BY_US"));
        Double DAYS_TO_CLOSE_DATE__C = handleDouble(resultSet1.getString("DAYS_TO_CLOSE_DATE__C"));
        Double DAYS_TO_DESIGN_IN__C = handleDouble(resultSet1.getString("DAYS_TO_DESIGN_IN__C"));
        String DESCRIPTION = resultSet1.getString("DESCRIPTION");
        Date DESIGN_IN_DATE__C = handleDate(resultSet1.getString("DESIGN_IN_DATE__C"));
        String DISPLAY_SIZE_TYPE__C = resultSet1.getString("DISPLAY_SIZE_TYPE__C");
        String DIVISION__C = resultSet1.getString("DIVISION__C");
        Date EARLY_SAMPLE_DATE__C = handleDate(resultSet1.getString("EARLY_SAMPLE_DATE__C"));
        String ENGAGEMENT_MODEL__C = resultSet1.getString("ENGAGEMENT_MODEL__C");
        Double ESTIMATED_REVENUE__C = handleDouble(resultSet1.getString("ESTIMATED_REVENUE__C"));
        Date EXPECTED_DESIGN_WIN_DATE__C =
            handleDate(resultSet1.getString("EXPECTED_DESIGN_WIN_DATE__C"));
        Date EXPECTED_RFI_RFQ_DATE__C =
            handleDate(resultSet1.getString("EXPECTED_RFI_RFQ_DATE__C"));
        String FIRST_CUSTOMER_PO_NUMBER__C = resultSet1.getString("FIRST_CUSTOMER_PO_NUMBER__C");
        String FORECASTCATEGORY = resultSet1.getString("FORECASTCATEGORY");
        String FORECASTCATEGORYNAME = resultSet1.getString("FORECASTCATEGORYNAME");
        Double FORECAST_QUOTE_PRIMARY_PRODUCT =
            handleDouble(resultSet1.getString("FORECAST_QUOTE_PRIMARY_PRODUCT"));
        String FORECAST_QUOTE__C = resultSet1.getString("FORECAST_QUOTE__C");
        String FY16_QUOTE__C = resultSet1.getString("FY16_QUOTE__C");
        String GESTURES_CAPABILITIES__C = resultSet1.getString("GESTURES_CAPABILITIES__C");
        String GESTURE_SUPPORT__C = resultSet1.getString("GESTURE_SUPPORT__C");
        String HALOGEN_FREE_SUPPORT__C = resultSet1.getString("HALOGEN_FREE_SUPPORT__C");
        String HASOPENACTIVITY = resultSet1.getString("HASOPENACTIVITY");
        String HASOPPORTUNITYLINEITEM = resultSet1.getString("HASOPPORTUNITYLINEITEM");
        String HASOVERDUETASK = resultSet1.getString("HASOVERDUETASK");
        String ID = resultSet1.getString("ID");
        String INTERFACE__C = resultSet1.getString("INTERFACE__C");
        String ISCLOSED = resultSet1.getString("ISCLOSED");
        String ISDELETED = resultSet1.getString("ISDELETED");
        String ISPRIVATE = resultSet1.getString("ISPRIVATE");
        String ISWON = resultSet1.getString("ISWON");
        String JIRA_NUMBER__C = resultSet1.getString("JIRA_NUMBER__C");
        Date LASTACTIVITYDATE = handleDate(resultSet1.getString("LASTACTIVITYDATE"));
        String LASTMODIFIEDBYID = resultSet1.getString("LASTMODIFIEDBYID");
        Timestamp LASTMODIFIEDDATE = handleTimestamp(resultSet1.getString("LASTMODIFIEDDATE"));
        Timestamp LASTREFERENCEDDATE = handleTimestamp(resultSet1.getString("LASTREFERENCEDDATE"));
        Timestamp LASTVIEWEDDATE = handleTimestamp(resultSet1.getString("LASTVIEWEDDATE"));
        Date LAST_UPDATED_BY_USER__C = handleDate(resultSet1.getString("LAST_UPDATED_BY_USER__C"));
        String LCM_OPPORTUNITY_LOOKUP__C = resultSet1.getString("LCM_OPPORTUNITY_LOOKUP__C");
        String LCM_REFERENCE_ACCOUNT__C = resultSet1.getString("LCM_REFERENCE_ACCOUNT__C");
        String LCM_TO_OEM_REFERENCE_ACCOUNT__ =
            resultSet1.getString("LCM_TO_OEM_REFERENCE_ACCOUNT__");
        String LCM_TO_OEM_REFERENCE_OPPORTUNI =
            resultSet1.getString("LCM_TO_OEM_REFERENCE_OPPORTUNI");
        String LEADSOURCE = resultSet1.getString("LEADSOURCE");
        String LIFETIME_QUANTITY_TIER__C = resultSet1.getString("LIFETIME_QUANTITY_TIER__C");
        String LIFETIME_QUANTITY__C = resultSet1.getString("LIFETIME_QUANTITY__C");
        String MARKET_SEGMENT_CODE__C = resultSet1.getString("MARKET_SEGMENT_CODE__C");
        String MOBILE_OS__C = resultSet1.getString("MOBILE_OS__C");
        String MODEL_NAME_IN_MARKET__C = resultSet1.getString("MODEL_NAME_IN_MARKET__C");
        String MOQ__C = resultSet1.getString("MOQ__C");
        Date MP_DATE__C = handleDate(resultSet1.getString("MP_DATE__C"));
        String MP_FORECAST_FLAG__C = resultSet1.getString("MP_FORECAST_FLAG__C");
        String NAME = resultSet1.getString("NAME");
        String NEXTSTEP = resultSet1.getString("NEXTSTEP");
        String NOTEBOOK_LCD_SIZE__C = resultSet1.getString("NOTEBOOK_LCD_SIZE__C");
        Double NUMBER_OF_COMPETITORS__C =
            handleDouble(resultSet1.getString("NUMBER_OF_COMPETITORS__C"));
        Double NUMBER_OF_DAYS_PAST_DUE__C =
            handleDouble(resultSet1.getString("NUMBER_OF_DAYS_PAST_DUE__C"));
        Double NUMBER_OF_MONTHS_LIFETIME__C =
            handleDouble(resultSet1.getString("NUMBER_OF_MONTHS_LIFETIME__C"));
        Double NUMBER_OF_OPPORTUNITIES__C =
            handleDouble(resultSet1.getString("NUMBER_OF_OPPORTUNITIES__C"));
        Double NUMBER_OF_PRODUCTS__C = handleDouble(resultSet1.getString("NUMBER_OF_PRODUCTS__C"));
        Double NUMBER_OF_QUOTES_WITH_PRODUCTS =
            handleDouble(resultSet1.getString("NUMBER_OF_QUOTES_WITH_PRODUCTS"));
        Double NUMBER_OF_QUOTES__C = handleDouble(resultSet1.getString("NUMBER_OF_QUOTES__C"));
        String ODM_PROGRAM_NAME__C = resultSet1.getString("ODM_PROGRAM_NAME__C");
        String ODM_REFERENCE_ACCOUNT__C = resultSet1.getString("ODM_REFERENCE_ACCOUNT__C");
        String ODM_SUGGESTED_FSF__C = resultSet1.getString("ODM_SUGGESTED_FSF__C");
        String OEM_OPPORTUNITY_LOOKUP__C = resultSet1.getString("OEM_OPPORTUNITY_LOOKUP__C");
        String OEM_OPPORTUNITY_NAME__C = resultSet1.getString("OEM_OPPORTUNITY_NAME__C");
        String OEM_PROGRAM_NAME_INPUT__C = resultSet1.getString("OEM_PROGRAM_NAME_INPUT__C");
        String OEM_REFERENCE_ACCOUNT_ID__C = resultSet1.getString("OEM_REFERENCE_ACCOUNT_ID__C");
        String OEM_REFERENCE_ACCOUNT__C = resultSet1.getString("OEM_REFERENCE_ACCOUNT__C");
        Double OEM_SUGGESTED_FSF__C = handleDouble(resultSet1.getString("OEM_SUGGESTED_FSF__C"));
        String OEM_VAR_ACCOUNT__C = resultSet1.getString("OEM_VAR_ACCOUNT__C");
        Double OF_ADDITIONAL_QUOTES__C =
            handleDouble(resultSet1.getString("OF_ADDITIONAL_QUOTES__C"));
        Double OF_RELATED_ODM_S__C = handleDouble(resultSet1.getString("OF_RELATED_ODM_S__C"));
        String OID__C = resultSet1.getString("OID__C");
        String OMIT_FROM_FORECAST__C = resultSet1.getString("OMIT_FROM_FORECAST__C");
        Double OPA_SAME_ACCOUNT_ROLLUP__C =
            handleDouble(resultSet1.getString("OPA_SAME_ACCOUNT_ROLLUP__C"));
        String OPPORTUNITY_FORECAST__C = null;
        String OPPORTUNITY_LIFETIME_QUANTITY_ =
            resultSet1.getString("OPPORTUNITY_LIFETIME_QUANTITY_");
        String OPPORTUNITY_OWNER_EMP_ID__C = resultSet1.getString("OPPORTUNITY_OWNER_EMP_ID__C");
        String OPPORTUNITY_REPORT_NAME__C = resultSet1.getString("OPPORTUNITY_REPORT_NAME__C");
        Double OPPORTUNITY_STAGE_SCORE__C =
            handleDouble(resultSet1.getString("OPPORTUNITY_STAGE_SCORE__C"));
        Double OPPORTUNITY_VOLUME__C = handleDouble(resultSet1.getString("OPPORTUNITY_VOLUME__C"));
        String ORACLE_SALES_ORDER_NUMBER__C = resultSet1.getString("ORACLE_SALES_ORDER_NUMBER__C");
        String ORIGINAL_PART_NUMBER__C = resultSet1.getString("ORIGINAL_PART_NUMBER__C");
        String OS__C = resultSet1.getString("OS__C");
        String OTHER_INTERFACE_DETAILS__C = resultSet1.getString("OTHER_INTERFACE_DETAILS__C");
        String OWNERID = resultSet1.getString("OWNERID");
        String PANEL_RESOLUTION__C = resultSet1.getString("PANEL_RESOLUTION__C");
        String PANEL_SIZE__C = resultSet1.getString("PANEL_SIZE__C");
        String PARENT_OEM_ACCOUNT__C = resultSet1.getString("PARENT_OEM_ACCOUNT__C");
        String PHONE_TYPE__C = resultSet1.getString("PHONE_TYPE__C");
        String PLANNING_POLICY__C = resultSet1.getString("PLANNING_POLICY__C");
        String PRECISION_TOUCHPAD__C = resultSet1.getString("PRECISION_TOUCHPAD__C");
        String PREFERRED_SUPPLIER_LENS__C = resultSet1.getString("PREFERRED_SUPPLIER_LENS__C");
        String PREFERRED_SUPPLIER_SENSOR__C = resultSet1.getString("PREFERRED_SUPPLIER_SENSOR__C");
        String PRICEBOOK2ID = resultSet1.getString("PRICEBOOK2ID");
        String PRICE_REDUCTION_CONSOLE__C = null;
        Double PRIMARY_APPROVED__C = handleDouble(resultSet1.getString("PRIMARY_APPROVED__C"));
        String PRIMARY_CONTACT__C = resultSet1.getString("PRIMARY_CONTACT__C");
        String PRIMARY_PRODUCT_STATUS__C = resultSet1.getString("PRIMARY_PRODUCT_STATUS__C");
        String PRIMARY_QUOTE_BILL_TO_NAME__C =
            resultSet1.getString("PRIMARY_QUOTE_BILL_TO_NAME__C");
        String PRIMARY_SOLUTION__C = resultSet1.getString("PRIMARY_SOLUTION__C");
        Double PROBABILITY = handleDouble(resultSet1.getString("PROBABILITY"));
        String PRODUCT_GROUP__C = resultSet1.getString("PRODUCT_GROUP__C");
        String PRODUCT_TYPE2__C = resultSet1.getString("PRODUCT_TYPE2__C");
        String PRODUCT__C = resultSet1.getString("PRODUCT__C");
        String PROJECT_ACCOUNT_NAME_S_REGION_ =
            resultSet1.getString("PROJECT_ACCOUNT_NAME_S_REGION_");
        String PROJECT_MODEL_NAME_IN_MARKET__ =
            resultSet1.getString("PROJECT_MODEL_NAME_IN_MARKET__");
        Double PROJECT_VOLUME__C = handleDouble(resultSet1.getString("PROJECT_VOLUME__C"));
        String PROJECT__C = resultSet1.getString("PROJECT__C");
        Date PURCHASE_ORDER_DATE__C = handleDate(resultSet1.getString("PURCHASE_ORDER_DATE__C"));
        Double QUANTITY_PER_MONTH__C = handleDouble(resultSet1.getString("QUANTITY_PER_MONTH__C"));
        String QUOTE_BILL_TO_PAYMENT_TERMS__C =
            resultSet1.getString("QUOTE_BILL_TO_PAYMENT_TERMS__C");
        String QUOTE_BILL_TO__C = resultSet1.getString("QUOTE_BILL_TO__C");
        String QUOTE_CONDITIONS__C = resultSet1.getString("QUOTE_CONDITIONS__C");
        Date QUOTE_SENT_TO_CUSTOMER__C =
            handleDate(resultSet1.getString("QUOTE_SENT_TO_CUSTOMER__C"));
        Double QUOTE_STAGE_VALUE_ROLLUP__C =
            handleDouble(resultSet1.getString("QUOTE_STAGE_VALUE_ROLLUP__C"));
        String RECORDTYPEID = resultSet1.getString("RECORDTYPEID");
        String REP__C = resultSet1.getString("REP__C");
        Timestamp REQUEST_FOR_BUDGETARY_QUOTE__C =
            handleTimestamp(resultSet1.getString("REQUEST_FOR_BUDGETARY_QUOTE__C"));
        Timestamp REQUEST_FOR_RE_QUOTE__C =
            handleTimestamp(resultSet1.getString("REQUEST_FOR_RE_QUOTE__C"));
        String RE_QUOTE__C = resultSet1.getString("RE_QUOTE__C");
        String RFQ__C = resultSet1.getString("RFQ__C");
        Timestamp ROLLUPCOMPETITORS__C =
            handleTimestamp(resultSet1.getString("ROLLUPCOMPETITORS__C"));
        Date ROLLUP_ACTUAL_DESIGN_WIN_DATE_ =
            handleDate(resultSet1.getString("ROLLUP_ACTUAL_DESIGN_WIN_DATE_"));
        String RUNNING_USER__C = resultSet1.getString("RUNNING_USER__C");
        String SALES_REGION__C = resultSet1.getString("SALES_REGION__C");
        String SEGMENTATION__C = resultSet1.getString("SEGMENTATION__C");
        String SFID__C = resultSet1.getString("SFID__C");
        String SHIPPING_TERMS__C = resultSet1.getString("SHIPPING_TERMS__C");
        String SINGLE_MULTI_SOURCE__C = resultSet1.getString("SINGLE_MULTI_SOURCE__C");
        Timestamp SOW_SENT_TO_BU__C = handleTimestamp(resultSet1.getString("SOW_SENT_TO_BU__C"));
        String STAGENAME = resultSet1.getString("STAGENAME");
        String STANDARDIZED_OPPORTUNITY_NAME_ =
            resultSet1.getString("STANDARDIZED_OPPORTUNITY_NAME_");
        String STRATEGIC__C = resultSet1.getString("STRATEGIC__C");
        String SUBMIT_FOR_QUOTE__C = resultSet1.getString("SUBMIT_FOR_QUOTE__C");
        String SYNAPTICS_SHARE_PERCENTAGE__C =
            resultSet1.getString("SYNAPTICS_SHARE_PERCENTAGE__C");
        String SYNA_PART_NUMBER__C = resultSet1.getString("SYNA_PART_NUMBER__C");
        Double SYNA_SHARE__C = handleDouble(resultSet1.getString("SYNA_SHARE__C"));
        String SYNCEDQUOTEID = resultSet1.getString("SYNCEDQUOTEID");
        Timestamp SYSTEMMODSTAMP = handleTimestamp(resultSet1.getString("SYSTEMMODSTAMP"));
        Double TOTAL_CUSTOMER_PROJECT_QTY__C =
            handleDouble(resultSet1.getString("TOTAL_CUSTOMER_PROJECT_QTY__C"));
        Double TOTAL_OPPORTUNITY_FAE_INVOLVEM =
            handleDouble(resultSet1.getString("TOTAL_OPPORTUNITY_FAE_INVOLVEM"));
        String TYPE = resultSet1.getString("TYPE");
        String WHITEBOX__C = resultSet1.getString("WHITEBOX__C");
        Double WIN_RATE__C = handleDouble(resultSet1.getString("WIN_RATE__C"));
        String FORECAST_QU_PRI_PRO_NAME_C = resultSet1.getString("FORECAST_QU_PRI_PRO_NAME_C");
        String STAND_OPPO_NAME_INDEXED_C = resultSet1.getString("STAND_OPPO_NAME_INDEXED_C");
        insertOpportunity(ACCOUNTID, ACTUAL_DESIGN_IN_DATE__C, ACTUAL_SYNA_MP_DATE__C,
            ACTUALS_AMOUNT__C, AMOUNT, APPLICATION__C, APTS_ALLOW_OPPORTUNITY_STAGE_C,
            APTS_PROJECT_TYPE__C, APTTUS_APPROVAL__APPROVAL_STAT, APTTUS_PROPOSAL__DISCOUNT_PERC,
            ASP__C, AUDIT_ACTUAL_DW_VS_CLOSEDATE__, AVERAGE_MONTHLY_QUANTITY__C,
            BU_PRICE_APPROVED_DATE__C, BUDGETARY__C, BUDGETARY_PRICE_APPROVAL_DATE_, CAMPAIGNID,
            CHANNEL__C, CLEARPAD_SERIES__C, CLOSED_DATE_PAST_DUE__C, CLOSEDATE,
            COUNT_OF_BU_REQUESTED_TIMESTAM, CREATEDBYID, CREATEDDATE, CURRENT_SALES_NET_PRICE__C,
            CUSTOMER_ID__C, CUSTOMER_PART_NUMBER__C, CUSTOMER_REQUIREMENTS__C,
            CUSTOMER_SPECIFIC_QUOTE_CONDIT, DAYS_SINCE_LAST_MODIFIED_BY_US, DAYS_TO_CLOSE_DATE__C,
            DAYS_TO_DESIGN_IN__C, DESCRIPTION, DESIGN_IN_DATE__C, DISPLAY_SIZE_TYPE__C, DIVISION__C,
            EARLY_SAMPLE_DATE__C, ENGAGEMENT_MODEL__C, ESTIMATED_REVENUE__C,
            EXPECTED_DESIGN_WIN_DATE__C, EXPECTED_RFI_RFQ_DATE__C, FIRST_CUSTOMER_PO_NUMBER__C,
            FORECAST_QUOTE__C, FORECAST_QUOTE_PRIMARY_PRODUCT, FORECASTCATEGORY,
            FORECASTCATEGORYNAME, FY16_QUOTE__C, GESTURE_SUPPORT__C, GESTURES_CAPABILITIES__C,
            HALOGEN_FREE_SUPPORT__C, HASOPENACTIVITY, HASOPPORTUNITYLINEITEM, HASOVERDUETASK, ID,
            INTERFACE__C, ISCLOSED, ISDELETED, ISPRIVATE, ISWON, JIRA_NUMBER__C,
            LAST_UPDATED_BY_USER__C, LASTACTIVITYDATE, LASTMODIFIEDBYID, LASTMODIFIEDDATE,
            LASTREFERENCEDDATE, LASTVIEWEDDATE, LCM_OPPORTUNITY_LOOKUP__C, LCM_REFERENCE_ACCOUNT__C,
            LCM_TO_OEM_REFERENCE_ACCOUNT__, LCM_TO_OEM_REFERENCE_OPPORTUNI, LEADSOURCE,
            LIFETIME_QUANTITY__C, LIFETIME_QUANTITY_TIER__C, MARKET_SEGMENT_CODE__C, MOBILE_OS__C,
            MODEL_NAME_IN_MARKET__C, MOQ__C, MP_DATE__C, MP_FORECAST_FLAG__C, NAME, NEXTSTEP,
            NOTEBOOK_LCD_SIZE__C, NUMBER_OF_COMPETITORS__C, NUMBER_OF_DAYS_PAST_DUE__C,
            NUMBER_OF_MONTHS_LIFETIME__C, NUMBER_OF_OPPORTUNITIES__C, NUMBER_OF_PRODUCTS__C,
            NUMBER_OF_QUOTES__C, NUMBER_OF_QUOTES_WITH_PRODUCTS, ODM_PROGRAM_NAME__C,
            ODM_REFERENCE_ACCOUNT__C, ODM_SUGGESTED_FSF__C, OEM_OPPORTUNITY_LOOKUP__C,
            OEM_OPPORTUNITY_NAME__C, OEM_PROGRAM_NAME_INPUT__C, OEM_REFERENCE_ACCOUNT__C,
            OEM_REFERENCE_ACCOUNT_ID__C, OEM_SUGGESTED_FSF__C, OEM_VAR_ACCOUNT__C,
            OF_ADDITIONAL_QUOTES__C, OF_RELATED_ODM_S__C, OID__C, OMIT_FROM_FORECAST__C,
            OPA_SAME_ACCOUNT_ROLLUP__C, OPPORTUNITY_FORECAST__C, OPPORTUNITY_LIFETIME_QUANTITY_,
            OPPORTUNITY_OWNER_EMP_ID__C, OPPORTUNITY_REPORT_NAME__C, OPPORTUNITY_STAGE_SCORE__C,
            OPPORTUNITY_VOLUME__C, ORACLE_SALES_ORDER_NUMBER__C, ORIGINAL_PART_NUMBER__C, OS__C,
            OTHER_INTERFACE_DETAILS__C, OWNERID, PANEL_RESOLUTION__C, PANEL_SIZE__C,
            PARENT_OEM_ACCOUNT__C, PHONE_TYPE__C, PLANNING_POLICY__C, PRECISION_TOUCHPAD__C,
            PREFERRED_SUPPLIER_LENS__C, PREFERRED_SUPPLIER_SENSOR__C, PRICE_REDUCTION_CONSOLE__C,
            PRICEBOOK2ID, PRIMARY_APPROVED__C, PRIMARY_CONTACT__C, PRIMARY_PRODUCT_STATUS__C,
            PRIMARY_QUOTE_BILL_TO_NAME__C, PRIMARY_SOLUTION__C, PROBABILITY, PRODUCT__C,
            PRODUCT_GROUP__C, PRODUCT_TYPE2__C, PROJECT__C, PROJECT_ACCOUNT_NAME_S_REGION_,
            PROJECT_MODEL_NAME_IN_MARKET__, PROJECT_VOLUME__C, PURCHASE_ORDER_DATE__C,
            QUANTITY_PER_MONTH__C, QUOTE_BILL_TO__C, QUOTE_BILL_TO_PAYMENT_TERMS__C,
            QUOTE_CONDITIONS__C, QUOTE_SENT_TO_CUSTOMER__C, QUOTE_STAGE_VALUE_ROLLUP__C,
            RE_QUOTE__C, RECORDTYPEID, REP__C, REQUEST_FOR_BUDGETARY_QUOTE__C,
            REQUEST_FOR_RE_QUOTE__C, RFQ__C, ROLLUP_ACTUAL_DESIGN_WIN_DATE_, ROLLUPCOMPETITORS__C,
            RUNNING_USER__C, SALES_REGION__C, SEGMENTATION__C, SFID__C, SHIPPING_TERMS__C,
            SINGLE_MULTI_SOURCE__C, SOW_SENT_TO_BU__C, STAGENAME, STANDARDIZED_OPPORTUNITY_NAME_,
            STRATEGIC__C, SUBMIT_FOR_QUOTE__C, SYNA_PART_NUMBER__C, SYNA_SHARE__C,
            SYNAPTICS_SHARE_PERCENTAGE__C, SYNCEDQUOTEID, SYSTEMMODSTAMP,
            TOTAL_CUSTOMER_PROJECT_QTY__C, TOTAL_OPPORTUNITY_FAE_INVOLVEM, TYPE, WHITEBOX__C,
            WIN_RATE__C, FORECAST_QU_PRI_PRO_NAME_C, STAND_OPPO_NAME_INDEXED_C);
        System.out.println("Inserting into Opportunity Id: " + ID);
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
      if (connection2 != null) {
        try {
          connection2.close();
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
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void insertOpportunity(String ACCOUNTID, Date ACTUAL_DESIGN_IN_DATE__C,
      Date ACTUAL_SYNA_MP_DATE__C, Double ACTUALS_AMOUNT__C, Double AMOUNT, String APPLICATION__C,
      String ALLOW_OPPORTUNITY_STG_CHNG__C, String APTS_PROJECT_TYPE__C,
      String APPROVAL__APPROVAL_STATUS__C, Double PROPOSAL__DISCOUNT_PERCENT__C, Double ASP__C,
      String AUDIT_ACTL_DW_VS_CLOSEDT__C, Double AVERAGE_MONTHLY_QUANTITY__C,
      Timestamp BUDGETARY_PRICE_APPROVAL_DT__C, String BUDGETARY__C,
      Timestamp BU_PRICE_APPROVED_DATE__C, String CAMPAIGNID, String CHANNEL__C,
      String CLEARPAD_SERIES__C, String CLOSED_DATE_PAST_DUE__C, Date CLOSEDATE,
      Double CNT_OF_BU_RQST_TIMESTP__C, String CREATEDBYID, Timestamp CREATEDDATE,
      Double CURRENT_SALES_NET_PRICE__C, String CUSTOMER_ID__C, String CUSTOMER_PART_NUMBER__C,
      String CUSTOMER_REQUIREMENTS__C, String CUSTOMER_SPEC_QUOTE_COND__C,
      Double DAYS_SINCE_LST_MOD_BY_USER__C, Double DAYS_TO_CLOSE_DATE__C,
      Double DAYS_TO_DESIGN_IN__C, String DESCRIPTION, Date DESIGN_IN_DATE__C,
      String DISPLAY_SIZE_TYPE__C, String DIVISION__C, Date EARLY_SAMPLE_DATE__C,
      String ENGAGEMENT_MODEL__C, Double ESTIMATED_REVENUE__C, Date EXPECTED_DESIGN_WIN_DATE__C,
      Date EXPECTED_RFI_RFQ_DATE__C, String FIRST_CUSTOMER_PO_NUMBER__C, String FORECAST_QUOTE__C,
      Double FCST_QUOTE_PRIM_PROD_WGHT__C, String FORECASTCATEGORY, String FORECASTCATEGORYNAME,
      String FY16_QUOTE__C, String GESTURE_SUPPORT__C, String GESTURES_CAPABILITIES__C,
      String HALOGEN_FREE_SUPPORT__C, String HASOPENACTIVITY, String HASOPPORTUNITYLINEITEM,
      String HASOVERDUETASK, String ID, String INTERFACE__C, String ISCLOSED, String ISDELETED,
      String ISPRIVATE, String ISWON, String JIRA_NUMBER__C, Date LAST_UPDATED_BY_USER__C,
      Date LASTACTIVITYDATE, String LASTMODIFIEDBY, Timestamp LASTMODIFIEDDATE,
      Timestamp LASTREFERENCEDDATE, Timestamp LASTVIEWEDDATE, String LCM_OPPORTUNITY_LOOKUP__C,
      String LCM_REFERENCE_ACCOUNT__C, String LCM_TO_OEM_REF_ACCT__C,
      String LCM_TO_OEM_REF_OPPORTUNITY__C, String LEADSOURCE, String LIFETIME_QUANTITY__C,
      String LIFETIME_QUANTITY_TIER__C, String MARKET_SEGMENT_CODE__C, String MOBILE_OS__C,
      String MODEL_NAME_IN_MARKET__C, String MOQ__C, Date MP_DATE__C, String MP_FORECAST_FLAG__C,
      String NAME, String NEXTSTEP, String NOTEBOOK_LCD_SIZE__C, Double NUMBER_OF_COMPETITORS__C,
      Double NUMBER_OF_DAYS_PAST_DUE__C, Double NUMBER_OF_MONTHS_LIFETIME__C,
      Double NUMBER_OF_OPPORTUNITIES__C, Double NUMBER_OF_PRODUCTS__C, Double NUMBER_OF_QUOTES__C,
      Double NUMBER_OF_QUOTES_WTH_PROD__C, String ODM_PROGRAM_NAME__C,
      String ODM_REFERENCE_ACCOUNT__C, String ODM_SUGGESTED_FSF__C,
      String OEM_OPPORTUNITY_LOOKUP__C, String OEM_OPPORTUNITY_NAME__C,
      String OEM_PROGRAM_NAME_INPUT__C, String OEM_REFERENCE_ACCOUNT__C,
      String OEM_REFERENCE_ACCT_ID__C, Double OEM_SUGGESTED_FSF__C, String OEM_VAR_ACCOUNT__C,
      Double OF_ADDITIONAL_QUOTES__C, Double OF_RELATED_ODM_S__C, String OID__C,
      String OMIT_FROM_FORECAST__C, Double OPA_SAME_ACCT_ROLLUP__C, String OPPORTUNITY_FORECAST__C,
      String OPPORTUNITY_LIFETIME_QTY__C, String OPPORTUNITY_OWNER_EMP_ID__C,
      String OPPORTUNITY_REPORT_NAME__C, Double OPPORTUNITY_STAGE_SCORE__C,
      Double OPPORTUNITY_VOLUME__C, String ORACLE_SALES_ORDER_NUMBER__C,
      String ORIGINAL_PART_NUMBER__C, String OS__C, String OTHER_INTERFACE_DETAILS__C,
      String OWNERID, String PANEL_RESOLUTION__C, String PANEL_SIZE__C,
      String PARENT_OEM_ACCOUNT__C, String PHONE_TYPE__C, String PLANNING_POLICY__C,
      String PRECISION_TOUCHPAD__C, String PREFERRED_SUPPLIER_LENS__C,
      String PREFERRED_SUPPLIER_SENSOR__C, String PRICE_REDUCTION_CONSOLE__C, String PRICEBOOK2ID,
      Double PRIMARY_APPROVED__C, String PRIMARY_CONTACT__C, String PRIMARY_PRODUCT_STATUS__C,
      String PRIMARY_QUOTE_BILL_TO_NAME__C, String PRIMARY_SOLUTION__C, Double PROBABILITY,
      String PRODUCT__C, String PRODUCT_GROUP__C, String PRODUCT_TYPE2__C, String PROJECT__C,
      String PROJ_ACCT_NAME_S_REGION__C, String PROJ_MODEL_NAME_IN_MKET__C,
      Double PROJECT_VOLUME__C, Date PURCHASE_ORDER_DATE__C, Double QUANTITY_PER_MONTH__C,
      String QUOTE_BILL_TO__C, String QUOTE_BILL_TO_PAYMENT_TERMS__C, String QUOTE_CONDITIONS__C,
      Date QUOTE_SENT_TO_CUSTOMER__C, Double QUOTE_STAGE_VALUE_ROLLUP__C, String RE_QUOTE__C,
      String RECORDTYPEID, String REP__C, Timestamp REQUEST_FOR_BUDGETARY_QUOTE__C,
      Timestamp REQUEST_FOR_RE_QUOTE__C, String RFQ__C, Date ROLLUP_ACTL_DSGN_WIN_DT__C,
      Timestamp ROLLUPCOMPETITORS__C, String RUNNING_USER__C, String SALES_REGION__C,
      String SEGMENTATION__C, String SFID__C, String SHIPPING_TERMS__C,
      String SINGLE_MULTI_SOURCE__C, Timestamp SOW_SENT_TO_BU__C, String STAGENAME,
      String STAND_OPPORTUNITY_NAME__C, String STRATEGIC__C, String SUBMIT_FOR_QUOTE__C,
      String SYNA_PART_NUMBER__C, Double SYNA_SHARE__C, String SYNAPTICS_SHARE_PERCENTAGE__C,
      String SYNCEDQUOTEID, Timestamp SYSTEMMODSTAMP, Double TOTAL_CUSTOMER_PROJECT_QTY__C,
      Double TOTL_OPPORTUNITY_FAE_INVOL__C, String TYPE, String WHITEBOX__C, Double WIN_RATE__C,
      String FORECAST_QU_PRI_PRO_NAME_C, String STAND_OPPO_NAME_INDEXED_C) {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1.prepareStatement(
          "INSERT INTO OPPORTUNITY(ACCOUNTID, ACTUALS_AMOUNT__C, ACTUAL_DESIGN_IN_DATE__C, "
              + "ACTUAL_SYNA_MP_DATE__C, AMOUNT, APPLICATION__C, ALLOW_OPPORTUNITY_STG_CHNG__C, "
              + "APTS_PROJECT_TYPE__C, APPROVAL__APPROVAL_STATUS__C,"
              + " PROPOSAL__DISCOUNT_PERCENT__C, "
              + "ASP__C, AUDIT_ACTL_DW_VS_CLOSEDT__C, AVERAGE_MONTHLY_QUANTITY__C, "
              + "BU_PRICE_APPROVED_DATE__C, BUDGETARY__C, BUDGETARY_PRICE_APPROVAL_DT__C, "
              + "CAMPAIGNID, CHANNEL__C, CLEARPAD_SERIES__C, CLOSEDATE, CLOSED_DATE_PAST_DUE__C, "
              + "CNT_OF_BU_RQST_TIMESTP__C, CREATEDBYID, CREATEDDATE, CURRENT_SALES_NET_PRICE__C, "
              + "CUSTOMER_ID__C, CUSTOMER_PART_NUMBER__C, CUSTOMER_REQUIREMENTS__C, "
              + "CUSTOMER_SPEC_QUOTE_COND__C, DAYS_SINCE_LST_MOD_BY_USER__C, "
              + "DAYS_TO_CLOSE_DATE__C, "
              + "DAYS_TO_DESIGN_IN__C, DESCRIPTION, DESIGN_IN_DATE__C, DISPLAY_SIZE_TYPE__C, "
              + "DIVISION__C, EARLY_SAMPLE_DATE__C, ENGAGEMENT_MODEL__C, ESTIMATED_REVENUE__C, "
              + "EXPECTED_DESIGN_WIN_DATE__C, EXPECTED_RFI_RFQ_DATE__C,"
              + " FIRST_CUSTOMER_PO_NUMBER__C, "
              + "FORECASTCATEGORY, FORECASTCATEGORYNAME, FCST_QUOTE_PRIM_PROD_WGHT__C, "
              + "FORECAST_QUOTE__C, FY16_QUOTE__C, GESTURES_CAPABILITIES__C, GESTURE_SUPPORT__C, "
              + "HALOGEN_FREE_SUPPORT__C, HASOPENACTIVITY, HASOPPORTUNITYLINEITEM, HASOVERDUETASK, "
              + "ID, INTERFACE__C, ISCLOSED, ISDELETED, ISPRIVATE, ISWON, JIRA_NUMBER__C, "
              + "LASTACTIVITYDATE, LASTMODIFIEDBY, LASTMODIFIEDDATE, LASTREFERENCEDDATE, "
              + "LASTVIEWEDDATE, LAST_UPDATED_BY_USER__C, LCM_OPPORTUNITY_LOOKUP__C, "
              + "LCM_REFERENCE_ACCOUNT__C, LCM_TO_OEM_REF_ACCT__C, LCM_TO_OEM_REF_OPPORTUNITY__C, "
              + "LEADSOURCE, LIFETIME_QUANTITY_TIER__C, LIFETIME_QUANTITY__C, "
              + "MARKET_SEGMENT_CODE__C, "
              + "MOBILE_OS__C, MODEL_NAME_IN_MARKET__C, MOQ__C, MP_DATE__C, MP_FORECAST_FLAG__C, "
              + "NAME, NEXTSTEP, NOTEBOOK_LCD_SIZE__C, NUMBER_OF_COMPETITORS__C, "
              + "NUMBER_OF_DAYS_PAST_DUE__C, NUMBER_OF_MONTHS_LIFETIME__C, "
              + "NUMBER_OF_OPPORTUNITIES__C, "
              + "NUMBER_OF_PRODUCTS__C, NUMBER_OF_QUOTES_WTH_PROD__C, NUMBER_OF_QUOTES__C, "
              + "ODM_PROGRAM_NAME__C, ODM_REFERENCE_ACCOUNT__C, ODM_SUGGESTED_FSF__C, "
              + "OEM_OPPORTUNITY_LOOKUP__C, OEM_OPPORTUNITY_NAME__C, OEM_PROGRAM_NAME_INPUT__C, "
              + "OEM_REFERENCE_ACCT_ID__C, OEM_REFERENCE_ACCOUNT__C, OEM_SUGGESTED_FSF__C, "
              + "OEM_VAR_ACCOUNT__C, OF_ADDITIONAL_QUOTES__C, OF_RELATED_ODM_S__C, OID__C, "
              + "OMIT_FROM_FORECAST__C, OPA_SAME_ACCT_ROLLUP__C, OPPORTUNITY_FORECAST__C, "
              + "OPPORTUNITY_LIFETIME_QTY__C, OPPORTUNITY_OWNER_EMP_ID__C, "
              + "OPPORTUNITY_REPORT_NAME__C, "
              + "OPPORTUNITY_STAGE_SCORE__C, OPPORTUNITY_VOLUME__C, ORACLE_SALES_ORDER_NUMBER__C, "
              + "ORIGINAL_PART_NUMBER__C, OS__C, OTHER_INTERFACE_DETAILS__C, OWNERID, "
              + "PANEL_RESOLUTION__C, PANEL_SIZE__C, PARENT_OEM_ACCOUNT__C, PHONE_TYPE__C, "
              + "PLANNING_POLICY__C, PRECISION_TOUCHPAD__C, PREFERRED_SUPPLIER_LENS__C, "
              + "PREFERRED_SUPPLIER_SENSOR__C, PRICEBOOK2ID, PRICE_REDUCTION_CONSOLE__C, "
              + "PRIMARY_APPROVED__C, PRIMARY_CONTACT__C, PRIMARY_PRODUCT_STATUS__C, "
              + "PRIMARY_QUOTE_BILL_TO_NAME__C, PRIMARY_SOLUTION__C, PROBABILITY, "
              + "PRODUCT_GROUP__C, " + "PRODUCT_TYPE2__C, PRODUCT__C, PROJ_ACCT_NAME_S_REGION__C, "
              + "PROJ_MODEL_NAME_IN_MKET__C, "
              + "PROJECT_VOLUME__C, PROJECT__C, PURCHASE_ORDER_DATE__C, QUANTITY_PER_MONTH__C, "
              + "QUOTE_BILL_TO_PAYMENT_TERMS__C, QUOTE_BILL_TO__C, QUOTE_CONDITIONS__C, "
              + "QUOTE_SENT_TO_CUSTOMER__C, QUOTE_STAGE_VALUE_ROLLUP__C, RECORDTYPEID, REP__C, "
              + "REQUEST_FOR_BUDGETARY_QUOTE__C, REQUEST_FOR_RE_QUOTE__C, RE_QUOTE__C, RFQ__C, "
              + "ROLLUPCOMPETITORS__C, ROLLUP_ACTL_DSGN_WIN_DT__C, RUNNING_USER__C, "
              + "SALES_REGION__C, "
              + "SEGMENTATION__C, SFID__C, SHIPPING_TERMS__C, SINGLE_MULTI_SOURCE__C, "
              + "SOW_SENT_TO_BU__C, STAGENAME, STAND_OPPORTUNITY_NAME__C, STRATEGIC__C, "
              + "SUBMIT_FOR_QUOTE__C, SYNAPTICS_SHARE_PERCENTAGE__C, SYNA_PART_NUMBER__C, "
              + "SYNA_SHARE__C, SYNCEDQUOTEID, SYSTEMMODSTAMP, TOTAL_CUSTOMER_PROJECT_QTY__C, "
              + "TOTL_OPPORTUNITY_FAE_INVOL__C, TYPE, WHITEBOX__C, WIN_RATE__C, "
              + "FORECAST_QU_PRI_PRO_NAME_C, STAND_OPPO_NAME_INDEXED_C)"
              + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
              + "?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
              + " ?, ?, ?, "
              + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
              + " ?, ?, ?, " + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      preparedStatement1.setString(1, ACCOUNTID);
      preparedStatement1.setDouble(2, ACTUALS_AMOUNT__C);
      preparedStatement1.setDate(3, ACTUAL_DESIGN_IN_DATE__C);
      preparedStatement1.setDate(4, ACTUAL_SYNA_MP_DATE__C);
      preparedStatement1.setDouble(5, AMOUNT);
      preparedStatement1.setString(6, APPLICATION__C);
      preparedStatement1.setString(7, ALLOW_OPPORTUNITY_STG_CHNG__C);
      preparedStatement1.setString(8, APTS_PROJECT_TYPE__C);
      preparedStatement1.setString(9, APPROVAL__APPROVAL_STATUS__C);
      preparedStatement1.setDouble(10, PROPOSAL__DISCOUNT_PERCENT__C);
      preparedStatement1.setDouble(11, ASP__C);
      preparedStatement1.setString(12, AUDIT_ACTL_DW_VS_CLOSEDT__C);
      preparedStatement1.setDouble(13, AVERAGE_MONTHLY_QUANTITY__C);
      preparedStatement1.setTimestamp(14, BU_PRICE_APPROVED_DATE__C);
      preparedStatement1.setString(15, BUDGETARY__C);
      preparedStatement1.setTimestamp(16, BUDGETARY_PRICE_APPROVAL_DT__C);
      preparedStatement1.setString(17, CAMPAIGNID);
      preparedStatement1.setString(18, CHANNEL__C);
      preparedStatement1.setString(19, CLEARPAD_SERIES__C);
      preparedStatement1.setDate(20, CLOSEDATE);
      preparedStatement1.setString(21, CLOSED_DATE_PAST_DUE__C);
      preparedStatement1.setDouble(22, CNT_OF_BU_RQST_TIMESTP__C);
      preparedStatement1.setString(23, CREATEDBYID);
      preparedStatement1.setTimestamp(24, CREATEDDATE);
      preparedStatement1.setDouble(25, CURRENT_SALES_NET_PRICE__C);
      preparedStatement1.setString(26, CUSTOMER_ID__C);
      preparedStatement1.setString(27, CUSTOMER_PART_NUMBER__C);
      preparedStatement1.setString(28, "null");
      preparedStatement1.setString(29, CUSTOMER_SPEC_QUOTE_COND__C);
      preparedStatement1.setDouble(30, DAYS_SINCE_LST_MOD_BY_USER__C);
      preparedStatement1.setDouble(31, DAYS_TO_CLOSE_DATE__C);
      preparedStatement1.setDouble(32, DAYS_TO_DESIGN_IN__C);
      preparedStatement1.setString(33, DESCRIPTION);
      preparedStatement1.setDate(34, DESIGN_IN_DATE__C);
      preparedStatement1.setString(35, DISPLAY_SIZE_TYPE__C);
      preparedStatement1.setString(36, DIVISION__C);
      preparedStatement1.setDate(37, EARLY_SAMPLE_DATE__C);
      preparedStatement1.setString(38, ENGAGEMENT_MODEL__C);
      preparedStatement1.setDouble(39, ESTIMATED_REVENUE__C);
      preparedStatement1.setDate(40, EXPECTED_DESIGN_WIN_DATE__C);
      preparedStatement1.setDate(41, EXPECTED_RFI_RFQ_DATE__C);
      preparedStatement1.setString(42, FIRST_CUSTOMER_PO_NUMBER__C);
      preparedStatement1.setString(43, FORECASTCATEGORY);
      preparedStatement1.setString(44, FORECASTCATEGORYNAME);
      preparedStatement1.setDouble(45, FCST_QUOTE_PRIM_PROD_WGHT__C);
      preparedStatement1.setString(46, FORECAST_QUOTE__C);
      preparedStatement1.setString(47, FY16_QUOTE__C);
      preparedStatement1.setString(48, GESTURES_CAPABILITIES__C);
      preparedStatement1.setString(49, GESTURE_SUPPORT__C);
      preparedStatement1.setString(50, HALOGEN_FREE_SUPPORT__C);
      preparedStatement1.setString(51, HASOPENACTIVITY);
      preparedStatement1.setString(52, HASOPPORTUNITYLINEITEM);
      preparedStatement1.setString(53, HASOVERDUETASK);
      preparedStatement1.setString(54, ID);
      preparedStatement1.setString(55, INTERFACE__C);
      preparedStatement1.setString(56, ISCLOSED);
      preparedStatement1.setString(57, ISDELETED);
      preparedStatement1.setString(58, ISPRIVATE);
      preparedStatement1.setString(59, ISWON);
      preparedStatement1.setString(60, JIRA_NUMBER__C);
      preparedStatement1.setDate(61, LASTACTIVITYDATE);
      preparedStatement1.setString(62, LASTMODIFIEDBY);
      preparedStatement1.setTimestamp(63, LASTMODIFIEDDATE);
      preparedStatement1.setTimestamp(64, LASTREFERENCEDDATE);
      preparedStatement1.setTimestamp(65, LASTVIEWEDDATE);
      preparedStatement1.setDate(66, LAST_UPDATED_BY_USER__C);
      preparedStatement1.setString(67, LCM_OPPORTUNITY_LOOKUP__C);
      preparedStatement1.setString(68, LCM_REFERENCE_ACCOUNT__C);
      preparedStatement1.setString(69, LCM_TO_OEM_REF_ACCT__C);
      preparedStatement1.setString(70, LCM_TO_OEM_REF_OPPORTUNITY__C);
      preparedStatement1.setString(71, LEADSOURCE);
      preparedStatement1.setString(72, LIFETIME_QUANTITY_TIER__C);
      preparedStatement1.setString(73, LIFETIME_QUANTITY__C);
      preparedStatement1.setString(74, MARKET_SEGMENT_CODE__C);
      preparedStatement1.setString(75, MOBILE_OS__C);
      preparedStatement1.setString(76, MODEL_NAME_IN_MARKET__C);
      preparedStatement1.setString(77, MOQ__C);
      preparedStatement1.setDate(78, MP_DATE__C);
      preparedStatement1.setString(79, MP_FORECAST_FLAG__C);
      preparedStatement1.setString(80, NAME);
      preparedStatement1.setString(81, NEXTSTEP);
      preparedStatement1.setString(82, NOTEBOOK_LCD_SIZE__C);
      preparedStatement1.setDouble(83, NUMBER_OF_COMPETITORS__C);
      preparedStatement1.setDouble(84, NUMBER_OF_DAYS_PAST_DUE__C);
      preparedStatement1.setDouble(85, NUMBER_OF_MONTHS_LIFETIME__C);
      preparedStatement1.setDouble(86, NUMBER_OF_OPPORTUNITIES__C);
      preparedStatement1.setDouble(87, NUMBER_OF_PRODUCTS__C);
      preparedStatement1.setDouble(88, NUMBER_OF_QUOTES_WTH_PROD__C);
      preparedStatement1.setDouble(89, NUMBER_OF_QUOTES__C);
      preparedStatement1.setString(90, ODM_PROGRAM_NAME__C);
      preparedStatement1.setString(91, ODM_REFERENCE_ACCOUNT__C);
      preparedStatement1.setString(92, ODM_SUGGESTED_FSF__C);
      preparedStatement1.setString(93, OEM_OPPORTUNITY_LOOKUP__C);
      preparedStatement1.setString(94, OEM_OPPORTUNITY_NAME__C);
      preparedStatement1.setString(95, OEM_PROGRAM_NAME_INPUT__C);
      preparedStatement1.setString(96, OEM_REFERENCE_ACCT_ID__C);
      preparedStatement1.setString(97, OEM_REFERENCE_ACCOUNT__C);
      preparedStatement1.setDouble(98, OEM_SUGGESTED_FSF__C);
      preparedStatement1.setString(99, OEM_VAR_ACCOUNT__C);
      preparedStatement1.setDouble(100, OF_ADDITIONAL_QUOTES__C);
      preparedStatement1.setDouble(101, OF_RELATED_ODM_S__C);
      preparedStatement1.setString(102, OID__C);
      preparedStatement1.setString(103, OMIT_FROM_FORECAST__C);
      preparedStatement1.setDouble(104, OPA_SAME_ACCT_ROLLUP__C);
      preparedStatement1.setString(105, OPPORTUNITY_FORECAST__C);
      preparedStatement1.setString(106, OPPORTUNITY_LIFETIME_QTY__C);
      preparedStatement1.setString(107, OPPORTUNITY_OWNER_EMP_ID__C);
      preparedStatement1.setString(108, OPPORTUNITY_REPORT_NAME__C);
      preparedStatement1.setDouble(109, OPPORTUNITY_STAGE_SCORE__C);
      preparedStatement1.setDouble(110, OPPORTUNITY_VOLUME__C);
      preparedStatement1.setString(111, ORACLE_SALES_ORDER_NUMBER__C);
      preparedStatement1.setString(112, ORIGINAL_PART_NUMBER__C);
      preparedStatement1.setString(113, OS__C);
      preparedStatement1.setString(114, OTHER_INTERFACE_DETAILS__C);
      preparedStatement1.setString(115, OWNERID);
      preparedStatement1.setString(116, PANEL_RESOLUTION__C);
      preparedStatement1.setString(117, PANEL_SIZE__C);
      preparedStatement1.setString(118, PARENT_OEM_ACCOUNT__C);
      preparedStatement1.setString(119, PHONE_TYPE__C);
      preparedStatement1.setString(120, PLANNING_POLICY__C);
      preparedStatement1.setString(121, PRECISION_TOUCHPAD__C);
      preparedStatement1.setString(122, PREFERRED_SUPPLIER_LENS__C);
      preparedStatement1.setString(123, PREFERRED_SUPPLIER_SENSOR__C);
      preparedStatement1.setString(124, PRICEBOOK2ID);
      preparedStatement1.setString(125, PRICE_REDUCTION_CONSOLE__C);
      preparedStatement1.setDouble(126, PRIMARY_APPROVED__C);
      preparedStatement1.setString(127, PRIMARY_CONTACT__C);
      preparedStatement1.setString(128, PRIMARY_PRODUCT_STATUS__C);
      preparedStatement1.setString(129, PRIMARY_QUOTE_BILL_TO_NAME__C);
      preparedStatement1.setString(130, PRIMARY_SOLUTION__C);
      preparedStatement1.setDouble(131, PROBABILITY);
      preparedStatement1.setString(132, PRODUCT_GROUP__C);
      preparedStatement1.setString(133, PRODUCT_TYPE2__C);
      preparedStatement1.setString(134, PRODUCT__C);
      preparedStatement1.setString(135, PROJ_ACCT_NAME_S_REGION__C);
      preparedStatement1.setString(136, PROJ_MODEL_NAME_IN_MKET__C);
      preparedStatement1.setDouble(137, PROJECT_VOLUME__C);
      preparedStatement1.setString(138, PROJECT__C);
      preparedStatement1.setDate(139, PURCHASE_ORDER_DATE__C);
      preparedStatement1.setDouble(140, QUANTITY_PER_MONTH__C);
      preparedStatement1.setString(141, QUOTE_BILL_TO_PAYMENT_TERMS__C);
      preparedStatement1.setString(142, QUOTE_BILL_TO__C);
      preparedStatement1.setString(143, QUOTE_CONDITIONS__C);
      preparedStatement1.setDate(144, QUOTE_SENT_TO_CUSTOMER__C);
      preparedStatement1.setDouble(145, QUOTE_STAGE_VALUE_ROLLUP__C);
      preparedStatement1.setString(146, RECORDTYPEID);
      preparedStatement1.setString(147, REP__C);
      preparedStatement1.setTimestamp(148, REQUEST_FOR_BUDGETARY_QUOTE__C);
      preparedStatement1.setTimestamp(149, REQUEST_FOR_RE_QUOTE__C);
      preparedStatement1.setString(150, RE_QUOTE__C);
      preparedStatement1.setString(151, RFQ__C);
      preparedStatement1.setTimestamp(152, ROLLUPCOMPETITORS__C);
      preparedStatement1.setDate(153, ROLLUP_ACTL_DSGN_WIN_DT__C);
      preparedStatement1.setString(154, RUNNING_USER__C);
      preparedStatement1.setString(155, SALES_REGION__C);
      preparedStatement1.setString(156, SEGMENTATION__C);
      preparedStatement1.setString(157, SFID__C);
      preparedStatement1.setString(158, SHIPPING_TERMS__C);
      preparedStatement1.setString(159, SINGLE_MULTI_SOURCE__C);
      preparedStatement1.setTimestamp(160, SOW_SENT_TO_BU__C);
      preparedStatement1.setString(161, STAGENAME);
      preparedStatement1.setString(162, STAND_OPPORTUNITY_NAME__C);
      preparedStatement1.setString(163, STRATEGIC__C);
      preparedStatement1.setString(164, SUBMIT_FOR_QUOTE__C);
      preparedStatement1.setString(165, SYNAPTICS_SHARE_PERCENTAGE__C);
      preparedStatement1.setString(166, SYNA_PART_NUMBER__C);
      preparedStatement1.setDouble(167, SYNA_SHARE__C);
      preparedStatement1.setString(168, SYNCEDQUOTEID);
      preparedStatement1.setTimestamp(169, SYSTEMMODSTAMP);
      preparedStatement1.setDouble(170, TOTAL_CUSTOMER_PROJECT_QTY__C);
      preparedStatement1.setDouble(171, TOTL_OPPORTUNITY_FAE_INVOL__C);
      preparedStatement1.setString(172, TYPE);
      preparedStatement1.setString(173, WHITEBOX__C);
      preparedStatement1.setDouble(174, WIN_RATE__C);
      preparedStatement1.setString(175, FORECAST_QU_PRI_PRO_NAME_C);
      preparedStatement1.setString(176, STAND_OPPO_NAME_INDEXED_C);
      preparedStatement1.executeUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void queryRecordType() {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1.prepareStatement("Truncate Table RECORDTYPE");
      preparedStatement1.executeUpdate();
      QueryResult queryResults = sourceConnection
          .query("Select Id, Name, DeveloperName, NamespacePrefix, Description, BusinessProcessId, "
              + "SobjectType, IsActive, CreatedById, CreatedDate, LastModifiedById, "
              + "LastModifiedDate, SystemModstamp FROM RecordType");
      if (queryResults.getSize() > 0) {
        for (int i = 0; i < queryResults.getRecords().length; i++) {
          RecordType c = (RecordType) queryResults.getRecords()[i];
          String Id = c.getId();
          String Name = c.getName();
          String DeveloperName = c.getDeveloperName();
          String NamespacePrefix = c.getNamespacePrefix();
          String Description = c.getDescription();
          String BusinessProcessId = c.getBusinessProcessId();
          String SobjectType = c.getSobjectType();
          Boolean IsActive = c.getIsActive();
          String CreatedById = c.getCreatedById();
          Calendar CreatedDate_1 = c.getCreatedDate();
          Timestamp CreatedDate = handleTimestamp(CreatedDate_1);
          String LastModifiedById = c.getLastModifiedById();
          Calendar LastModifiedDate_1 = c.getLastModifiedDate();
          Timestamp LastModifiedDate = handleTimestamp(LastModifiedDate_1);
          Calendar SystemModstamp_1 = c.getSystemModstamp();
          Timestamp SystemModstamp = handleTimestamp(SystemModstamp_1);
          insertRecordType(Id, Name, DeveloperName, NamespacePrefix, Description, BusinessProcessId,
              SobjectType, IsActive, CreatedById, CreatedDate, LastModifiedById, LastModifiedDate,
              SystemModstamp);
          System.out.println("Inserting into RecordType Id: " + Id);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void insertRecordType(String Id, String Name, String DeveloperName,
      String NamespacePrefix, String Description, String BusinessProcessId, String SobjectType,
      Boolean IsActive, String CreatedById, Timestamp CreatedDate, String LastModifiedById,
      Timestamp LastModifiedDate, Timestamp SystemModstamp) {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1
          .prepareStatement("INSERT INTO RECORDTYPE(NAME,DEVELOPERNAME,NAMESPACEPREFIX,DESCRIPTION,"
              + "BUSINESSPROCESSID,SOBJECTTYPE,ISACTIVE,CREATEDBYID,CREATEDDATE,"
              + "LASTMODIFIEDBYID,LASTMODIFIEDDATE,SYSTEMMODSTAMP,ID)"
              + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)");
      preparedStatement1.setString(1, Name);
      preparedStatement1.setString(2, DeveloperName);
      preparedStatement1.setString(3, NamespacePrefix);
      preparedStatement1.setString(4, Description);
      preparedStatement1.setString(5, BusinessProcessId);
      preparedStatement1.setString(6, SobjectType);
      preparedStatement1.setBoolean(7, IsActive);
      preparedStatement1.setString(8, CreatedById);
      preparedStatement1.setTimestamp(9, CreatedDate);
      preparedStatement1.setString(10, LastModifiedById);
      preparedStatement1.setTimestamp(11, LastModifiedDate);
      preparedStatement1.setTimestamp(12, SystemModstamp);
      preparedStatement1.setString(13, Id);
      preparedStatement1.executeUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void querySynOppoFore() {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 =
          connection1.prepareStatement("Truncate Table SYN_OPPOR_PIPELINE_FORECAST__C");
      preparedStatement1.executeUpdate();
      QueryResult queryResults = sourceConnection
          .query("Select Id, OwnerId, IsDeleted, Name, CreatedDate, CreatedById,LastModifiedDate, "
              + "LastModifiedById, SystemModstamp, Forecast_Date__c, Opportunity__c, "
              + "Pipeline_Revenue__c, Quantity__c FROM SYN_Opportunity_Pipeline_Forecast__c ");
      boolean done = false;
      if (queryResults.getSize() > 0) {
        while (!done) {
          for (int i = 0; i < queryResults.getRecords().length; i++) {
            SYN_Opportunity_Pipeline_Forecast__c c =
                (SYN_Opportunity_Pipeline_Forecast__c) queryResults.getRecords()[i];
            String Id = c.getId();
            String OwnerId = c.getOwnerId();
            Boolean IsDeleted = c.getIsDeleted();
            String Name = c.getName();
            Calendar CreatedDate_1 = c.getCreatedDate();
            Timestamp CreatedDate = handleTimestamp(CreatedDate_1);
            String CreatedById = c.getCreatedById();
            Calendar LastModifiedDate_1 = c.getLastModifiedDate();
            Timestamp LastModifiedDate = handleTimestamp(LastModifiedDate_1);
            String LastModifiedById = c.getLastModifiedById();
            Calendar SystemModstamp_1 = c.getSystemModstamp();
            Timestamp SystemModstamp = handleTimestamp(SystemModstamp_1);
            Calendar Forecast_Date__c_1 = c.getForecast_Date__c();
            Timestamp Forecast_Date__c = handleTimestamp(Forecast_Date__c_1);
            String Opportunity__c = c.getOpportunity__c();
            Double Pipeline_Revenue__c_1 = c.getPipeline_Revenue__c();
            Double Pipeline_Revenue__c = handleDouble(Pipeline_Revenue__c_1);
            Double Quantity__c_1 = c.getQuantity__c();
            Double Quantity__c = handleDouble(Quantity__c_1);
            insertSynOppoFore(Id, OwnerId, IsDeleted, Name, CreatedDate, CreatedById,
                LastModifiedDate, LastModifiedById, SystemModstamp, Forecast_Date__c,
                Opportunity__c, Pipeline_Revenue__c, Quantity__c);
            System.out.println("Inserting into SynOppoFore Id: " + Id);
          }
          if (queryResults.isDone()) {
            done = true;
          } else {
            queryResults = sourceConnection.queryMore(queryResults.getQueryLocator());
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void insertSynOppoFore(String Id, String OwnerId, Boolean IsDeleted, String Name,
      Timestamp CreatedDate, String CreatedById, Timestamp LastModifiedDate,
      String LastModifiedById, Timestamp SystemModstamp, Timestamp Forecast_Date__c,
      String Opportunity__c, Double Pipeline_Revenue__c, Double Quantity__c) {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1
          .prepareStatement("INSERT INTO SYN_OPPOR_PIPELINE_FORECAST__C(ID,OWNERID,ISDELETED,NAME,"
              + "CREATEDDATE,CREATEDBYID,LASTMODIFIEDDATE,LASTMODIFIEDBYID,SYSTEMMODSTAMP,"
              + "FORECAST_DATE__C,OPPORTUNITY__C,PIPELINE_REVENUE__C,QUANTITY__C)"
              + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)");
      preparedStatement1.setString(1, Id);
      preparedStatement1.setString(2, OwnerId);
      preparedStatement1.setBoolean(3, IsDeleted);
      preparedStatement1.setString(4, Name);
      preparedStatement1.setTimestamp(5, CreatedDate);
      preparedStatement1.setString(6, CreatedById);
      preparedStatement1.setTimestamp(7, LastModifiedDate);
      preparedStatement1.setString(8, LastModifiedById);
      preparedStatement1.setTimestamp(9, SystemModstamp);
      preparedStatement1.setTimestamp(10, Forecast_Date__c);
      preparedStatement1.setString(11, Opportunity__c);
      preparedStatement1.setDouble(12, Pipeline_Revenue__c);
      preparedStatement1.setDouble(13, Quantity__c);
      preparedStatement1.executeUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void queryAPTSProForecast() {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1.prepareStatement("Truncate Table APTS_PRODUCT_FORECAST__C");
      preparedStatement1.executeUpdate();
      QueryResult queryResults =
          sourceConnection.query("Select Id, OwnerId, IsDeleted, Name,RecordTypeId, CreatedDate, "
              + "CreatedById, LastModifiedDate, LastModifiedById,SystemModstamp,"
              + "LastActivityDate,LastViewedDate,LastReferencedDate,"
              + "APTS_Price_Agreement__c, APTS_Product__c, APTS_Quantity__c,"
              + "APTS_Year_Quarter__c,AOP_Estimated_Price__c, Panel__c,"
              + "Product_is_primary__c, Record_Type_Name__c, Product_is_Secondary__c "
              + "FROM APTS_Product_Forecast__c");
      boolean done = false;
      if (queryResults.getSize() > 0) {
        while (!done) {
          for (int i = 0; i < queryResults.getRecords().length; i++) {
            APTS_Product_Forecast__c c = (APTS_Product_Forecast__c) queryResults.getRecords()[i];
            String Id = c.getId();
            String OwnerId = c.getOwnerId();
            Boolean IsDeleted = c.getIsDeleted();
            String Name = c.getName();
            String RecordTypeId = c.getRecordTypeId();
            Calendar CreatedDate_1 = c.getCreatedDate();
            Timestamp CreatedDate = handleTimestamp(CreatedDate_1);
            String CreatedById = c.getCreatedById();
            Calendar LastModifiedDate_1 = c.getLastModifiedDate();
            Timestamp LastModifiedDate = handleTimestamp(LastModifiedDate_1);
            String LastModifiedById = c.getLastModifiedById();
            Calendar SystemModstamp_1 = c.getSystemModstamp();
            Timestamp SystemModstamp = handleTimestamp(SystemModstamp_1);
            Calendar LastActivityDate_1 = c.getLastActivityDate();
            Date LastActivityDate = handleDate(LastActivityDate_1);
            Calendar LastViewedDate_1 = c.getLastViewedDate();
            Timestamp LastViewedDate = handleTimestamp(LastViewedDate_1);
            Calendar LastReferencedDate_1 = c.getLastReferencedDate();
            Timestamp LastReferencedDate = handleTimestamp(LastReferencedDate_1);
            String APTS_Price_Agreement__c = c.getAPTS_Price_Agreement__c();
            String APTS_Product__c = c.getAPTS_Product__c();
            Double APTS_Quantity__c_1 = c.getAPTS_Quantity__c();
            Double APTS_Quantity__c = handleDouble(APTS_Quantity__c_1);
            String APTS_Year_Quarter__c = c.getAPTS_Year_Quarter__c();
            Double AOP_Estimated_Price__c_1 = c.getAOP_Estimated_Price__c();
            Double AOP_Estimated_Price__c = handleDouble(AOP_Estimated_Price__c_1);
            String Panel__c = c.getPanel__c();
            Boolean Product_is_primary__c = c.getProduct_is_primary__c();
            String Record_Type_Name__c = c.getRecord_Type_Name__c();
            Boolean Product_is_Secondary__c = c.getProduct_is_Secondary__c();
            insertAPTSProForecast(Id, OwnerId, IsDeleted, Name, RecordTypeId, CreatedDate,
                CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate,
                LastViewedDate, LastReferencedDate, APTS_Price_Agreement__c, APTS_Product__c,
                APTS_Quantity__c, APTS_Year_Quarter__c, AOP_Estimated_Price__c, Panel__c,
                Product_is_primary__c, Record_Type_Name__c, Product_is_Secondary__c);
            System.out.println("Inserting into APTSProForecast Id: " + Id);
          }
          if (queryResults.isDone()) {
            done = true;
          } else {
            queryResults = sourceConnection.queryMore(queryResults.getQueryLocator());
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void insertAPTSProForecast(String Id, String OwnerId, Boolean IsDeleted,
      String Name, String RecordTypeId, Timestamp CreatedDate, String CreatedById,
      Timestamp LastModifiedDate, String LastModifiedById, Timestamp SystemModstamp,
      Date LastActivityDate, Timestamp LastViewedDate, Timestamp LastReferencedDate,
      String APTS_Price_Agreement__c, String APTS_Product__c, Double APTS_Quantity__c,
      String APTS_Year_Quarter__c, Double AOP_Estimated_Price__c, String Panel__c,
      Boolean Product_is_primary__c, String Record_Type_Name__c, Boolean Product_is_Secondary__c) {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1
          .prepareStatement("INSERT INTO APTS_PRODUCT_FORECAST__C(ID, OWNERID, ISDELETED, NAME, "
              + "RECORDTYPEID, CREATEDDATE, CREATEDBYID, LASTMODIFIEDDATE, LASTMODIFIEDBYID, "
              + "SYSTEMMODSTAMP, LASTACTIVITYDATE, LASTVIEWEDDATE, LASTREFERENCEDDATE, "
              + "APTS_PRICE_AGREEMENT__C, APTS_PRODUCT__C, APTS_QUANTITY__C, "
              + "APTS_YEAR_QUARTER__C, AOP_ESTIMATED_PRICE__C, PANEL__C, PRODUCT_IS_PRIMARY__C, "
              + "RECORD_TYPE_NAME__C, PRODUCT_IS_SECONDARY__C)"
              + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      preparedStatement1.setString(1, Id);
      preparedStatement1.setString(2, OwnerId);
      preparedStatement1.setBoolean(3, IsDeleted);
      preparedStatement1.setString(4, Name);
      preparedStatement1.setString(5, RecordTypeId);
      preparedStatement1.setTimestamp(6, CreatedDate);
      preparedStatement1.setString(7, CreatedById);
      preparedStatement1.setTimestamp(8, LastModifiedDate);
      preparedStatement1.setString(9, LastModifiedById);
      preparedStatement1.setTimestamp(10, SystemModstamp);
      preparedStatement1.setDate(11, LastActivityDate);
      preparedStatement1.setTimestamp(12, LastViewedDate);
      preparedStatement1.setTimestamp(13, LastReferencedDate);
      preparedStatement1.setString(14, APTS_Price_Agreement__c);
      preparedStatement1.setString(15, APTS_Product__c);
      preparedStatement1.setDouble(16, APTS_Quantity__c);
      preparedStatement1.setString(17, APTS_Year_Quarter__c);
      preparedStatement1.setDouble(18, AOP_Estimated_Price__c);
      preparedStatement1.setString(19, Panel__c);
      preparedStatement1.setBoolean(20, Product_is_primary__c);
      preparedStatement1.setString(21, Record_Type_Name__c);
      preparedStatement1.setBoolean(22, Product_is_Secondary__c);
      preparedStatement1.executeUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void queryOppoHistory() {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1.prepareStatement("Truncate Table OPPORTUNITY_HISTORY");
      preparedStatement1.executeUpdate();
      QueryResult queryResults =
          sourceConnection.query("Select Id, OpportunityId, CreatedById, CreatedDate, StageName, "
              + "Amount, ExpectedRevenue, CloseDate, Probability, ForecastCategory, "
              + "SystemModstamp, IsDeleted FROM OpportunityHistory");
      boolean done = false;
      if (queryResults.getSize() > 0) {
        while (!done) {
          for (int i = 0; i < queryResults.getRecords().length; i++) {
            OpportunityHistory c = (OpportunityHistory) queryResults.getRecords()[i];
            String Id = c.getId();
            String OpportunityId = c.getOpportunityId();
            String CreatedById = c.getCreatedById();
            Calendar CreatedDate_1 = c.getCreatedDate();
            Timestamp CreatedDate = handleTimestamp(CreatedDate_1);
            String StageName = c.getStageName();
            Double Amount_1 = c.getAmount();
            Double Amount = handleDouble(Amount_1);
            Double ExpectedRevenue_1 = c.getExpectedRevenue();
            Double ExpectedRevenue = handleDouble(ExpectedRevenue_1);
            Calendar CloseDate_1 = c.getCloseDate();
            Timestamp CloseDate = handleTimestamp(CloseDate_1);
            Double Probability_1 = c.getProbability();
            Double Probability = handleDouble(Probability_1);
            String ForecastCategory = c.getForecastCategory();
            Calendar SystemModstamp_1 = c.getSystemModstamp();
            Timestamp SystemModstamp = handleTimestamp(SystemModstamp_1);
            Boolean IsDeleted = c.getIsDeleted();
            insertOppoHistory(Id, OpportunityId, CreatedById, CreatedDate, StageName, Amount,
                ExpectedRevenue, CloseDate, Probability, ForecastCategory, SystemModstamp,
                IsDeleted);
            System.out.println("Inserting into OppoHistory Id: " + Id);
          }
          if (queryResults.isDone()) {
            done = true;
          } else {
            queryResults = sourceConnection.queryMore(queryResults.getQueryLocator());
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static void insertOppoHistory(String Id, String OpportunityId, String CreatedById,
      Timestamp CreatedDate, String StageName, Double Amount, Double ExpectedRevenue,
      Timestamp CloseDate, Double Probability, String ForecastCategory, Timestamp SystemModstamp,
      Boolean IsDeleted) {
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1.prepareStatement(
          "INSERT INTO OPPORTUNITY_HISTORY(ID, OPPORTUNITYID, CREATEDBYID, CREATEDDATE, "
              + "STAGENAME, AMOUNT, EXPECTEDREVENUE, CLOSEDATE, PROBABILITY, FORECASTCATEGORY, "
              + "SYSTEMMODSTAMP, ISDELETED)VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      preparedStatement1.setString(1, Id);
      preparedStatement1.setString(2, OpportunityId);
      preparedStatement1.setString(3, CreatedById);
      preparedStatement1.setTimestamp(4, CreatedDate);
      preparedStatement1.setString(5, StageName);
      preparedStatement1.setDouble(6, Amount);
      preparedStatement1.setDouble(7, ExpectedRevenue);
      preparedStatement1.setTimestamp(8, CloseDate);
      preparedStatement1.setDouble(9, Probability);
      preparedStatement1.setString(10, ForecastCategory);
      preparedStatement1.setTimestamp(11, SystemModstamp);
      preparedStatement1.setBoolean(12, IsDeleted);
      preparedStatement1.executeUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * To get the login credentials and lastRunnedTime.
   */
  private static void getPropertiesFile() {
    try {
      File file = new File("uinfo.properties");
      FileInputStream fileInput = new FileInputStream(file);
      Properties properties = new Properties();
      properties.load(fileInput);
      fileInput.close();
      usernameSource = properties.getProperty("usernameSource");
      passwordSource = properties.getProperty("passwordSource");
      usernameDestination = properties.getProperty("usernameDestination");
      passwordDestination = properties.getProperty("passwordDestination");
      lastRunnedTime = properties.getProperty("lastRunnedTime");
      System.out.println("Last Runned Time = " + lastRunnedTime);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * To get connection from destination.
   */
  private static Connection getConnection() {
    final String URL = "jdbc:oracle:thin:@sjc1uvt-obi03.synaptics.com:1522:BITST12C";
    final String USERNAME = usernameDestination;
    final String PASSWORD = passwordDestination;
    Connection connection = null;
    try {
      Class.forName("oracle.jdbc.driver.OracleDriver");
      connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
    return connection;
  }

  private static Double handleDouble(Double dlParam) {
    if (dlParam == null) {
      Double a = 0.0;
      return a;
    } else {
      Double yyy = dlParam;
      return yyy;
    }
  }

  private static Double handleDouble(String stParam) {
    if (stParam.equals("null")) {
      Double a = 0.0;
      return a;
    } else {
      Double yyy = Double.parseDouble(stParam);
      return yyy;
    }
  }

  private static Date handleDate(String stParam) {
    Date formatted = null;
    if (stParam.equals("null")) {
      formatted = null;
    } else {
      Calendar cal = Calendar.getInstance();
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
      try {
        cal.setTime(sdf.parse(stParam));
      } catch (ParseException e) {
        e.printStackTrace();
      }
      formatted = new Date(cal.getTimeInMillis());
    }
    return formatted;
  }

  private static Date handleDate(Calendar clParam) {
    Date formatted = null;
    if (clParam == null) {
      formatted = null;
    } else {
      formatted = new Date(clParam.getTimeInMillis());
    }
    return formatted;
  }

  private static Timestamp handleTimestamp(String stParam) {
    Timestamp formatted = null;
    if (stParam.equals("null")) {
      formatted = null;
    } else {
      Calendar cal = Calendar.getInstance();
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ENGLISH);
      try {
        cal.setTime(sdf.parse(stParam));
      } catch (ParseException e) {
        e.printStackTrace();
      }
      formatted = new Timestamp(cal.getTimeInMillis());
    }
    return formatted;
  }

  private static Timestamp handleTimestamp(Calendar clParam) {
    Timestamp formatted;
    if (clParam == null) {
      formatted = null;
    } else {
      formatted = new Timestamp(clParam.getTimeInMillis());
    }
    return formatted;
  }
}
