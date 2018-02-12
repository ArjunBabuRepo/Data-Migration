import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.soap.partner.sobject.*;

/*
 * Here we fetch the whole data from the required table from source(Salesforce db) and insert it
 * into the destination(Synaptics db).
 * Pass vm arguments “-Dhttps.protocols=TLSv1.1,TLSv1.2” while running the method.
 * Batch processing has been implemented, so rows are inserted in batches of 50.
 * Log files are implemented and mails are sent from linux system to all recepients
 * in the properties file along with the log file attachments.
 * In mail subject we get to know the server environment name and the result of the process.
 * To migrate to different environment we to only change the properties file.
 * Both the jobs are executed one after the other using shell scripting single command.
 * Comments are provided at necessary places only at the first occurrence of that situation.
 * General methods description:
 * 1) query*: These methods first truncate the respective tables and fetch the data from
 * Salesforce db and store it in the code. These methods also insert the fetched data in query*
 * to Synaptics db.
 * 2) handle*: These methods convert the datatypes into a format acceptable in Synaptics db. Also,
 * if a datatype is null we have to manually assign null or 0 depending on the situation.
 */
public final class PartnerWSDL {
  private static String usernameSource;
  private static String usernameDestination;
  private static String passwordSource;
  private static String passwordDestination;
  private static String urlSourcePartner;
  private static String urlDestination;
  private static String lastRunnedTime;
  private static String emailId;
  private static String DB_Name;
  private static String result = "Success";
  private static int batchSize = 50;
  private static String LogRunnedTime;
  private static String classname = "PatnerWSDL";
  private static PartnerConnection sourceConnection;
  private static PrintStream o;
  private static PrintStream console = System.out;

  public static void main(String[] args) throws FileNotFoundException {
    System.out.println("Partner Process Started");
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ENGLISH);
    Calendar cal = Calendar.getInstance();
    LogRunnedTime = dateFormat.format(cal.getTime());
    LogRunnedTime = LogRunnedTime.replaceAll(":", "-");
    try {
      o = new PrintStream(new File("Log_Folder/log_SFDC_" + classname + LogRunnedTime + ".txt"));
      System.setOut(o);
    } catch (FileNotFoundException e1) {
      e1.printStackTrace();
      result = "Failure";
    }
    long start = System.currentTimeMillis();
    System.out.println("StartTime for Partner wsdl=" + start);
    getPropertiesFile();
    ConnectorConfig config = new ConnectorConfig();
    config.setUsername(usernameSource);
    config.setPassword(passwordSource);
    config.setAuthEndpoint(urlSourcePartner);
    try {
      sourceConnection = Connector.newConnection(config);
      System.out.println("Auth EndPoint: " + config.getAuthEndpoint());
      System.out.println("Service EndPoint: " + config.getServiceEndpoint());
      System.out.println("Username: " + config.getUsername());
      System.out.println("SessionId: " + config.getSessionId());
      /*
       * Used for testing. Don't remove.
       */
      // queryAccount();
      // queryProduct2();
      // queryOpportunity();
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
      t5.start();
      t6.start();
      t7.start();
      t5.join();
      t6.join();
      t7.join();
    } catch (Exception e) {
      e.printStackTrace(o);
      result = "Failure";
    } finally {
      if (sourceConnection != null) {
        try {
          sourceConnection.logout();
        } catch (Exception e) {
          e.printStackTrace(o);
        }
      }
      System.out.println("Partner Process completed");
      long end = System.currentTimeMillis();
      System.out.println("EndTime for Partner wsdl=" + end);
      long TimeDiff = end - start;
      long minutes = (TimeDiff / 1000) / 60;
      System.out.println("TimeDiff in minute for Partner wsdl=" + minutes);
      try {
        o.close();
        System.setOut(console);
        System.out.println("Sending mail");
        Process process = Runtime.getRuntime()
            .exec("mail -a Log_Folder/log_SFDC_" + classname + LogRunnedTime
                + ".txt -s \"SFDC_Partner_wsdl_" + DB_Name + "_" + result + " process status\" "
                + emailId + " < /dev/null");
      } catch (Exception e) {
        e.printStackTrace();
      }
      System.out.println("Partner Process completed");
    }
  }

  @SuppressWarnings("resource")
  private static void queryAccount() {
    int count1 = 0;
    Connection connection1 = null;
    PreparedStatement preparedStatement1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1.prepareStatement("Truncate Table ACCOUNT_TEMP");
      preparedStatement1.executeUpdate();
      connection1.setAutoCommit(false);
      String soqlQuery =
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
              + "APTTUS_CONFIG2__BILLINGCONTACTFORMAT__C, "
              + "IHV__C, LCM__C, ODM__C, OEM__C, VAR__C, APTTUS_CONFIG2__BILLINGDAYOFMONTH__C, "
              + "APTTUS_CONFIG2__BILLINGPREFERENCEID__C, APTTUS_CONFIG2__CALENDARCYCLESTART__C, "
              + "APTTUS_CONFIG2__CERTIFICATEID__C, "
              + "APTTUS_CONFIG2__CREDITMEMOEMAILTEMPLATE__C, "
              + "APTTUS_CONFIG2__DEFAULTCREDITMEMOTEMPLATE__C, "
              + "APTTUS_CONFIG2__DEFAULTINVOICETEMPLATE__C, "
              + "APTTUS_CONFIG2__INVOICEEMAILTEMPLATE__C, " + "APTTUS_CONFIG2__PAYMENTTERMID__C, "
              + "APTTUS_CONFIG2__TAXEXEMPTSTATUS__C, APTTUS_CONFIG2__TAXEXEMPT__C, "
              + "APTTUS_CONFIG2__BILLINGCONTACTFILTERCRITERIA__C, APTTUS_CONFIG2__DEFAULTINVOICESTATEMENTTEMPLATE__C from Account";
      QueryResult qr = sourceConnection.query(soqlQuery);
      System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxx Account row count " + qr.getSize());
      preparedStatement1 = connection1
          .prepareStatement("INSERT INTO ACCOUNT_TEMP(ID, ISDELETED, MASTERRECORDID, NAME, "
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
      boolean done = false;
      if (qr.getSize() > 0) {
        while (!done) {
          SObject[] records = qr.getRecords();
          for (int i = 0; i < qr.getRecords().length; i++) {
            SObject s = records[i];
            String ID = s.getId();
            String ISDELETED = handleString(s.getField("IsDeleted")).toString();
            String MASTERRECORDID = handleString(s.getField("MasterRecordId")).toString();
            String NAME = handleString(s.getField("Name")).toString();
            String RECORDTYPEID = handleString(s.getField("RecordTypeId")).toString();
            String PARENTID = handleString(s.getField("ParentId")).toString();
            String BILLINGSTREET = handleString(s.getField("BillingStreet")).toString();
            String BILLINGCITY = handleString(s.getField("BillingCity")).toString();
            String BILLINGSTATE = handleString(s.getField("BillingState")).toString();
            String BILLINGPOSTALCODE = handleString(s.getField("BillingPostalCode")).toString();
            String BILLINGCOUNTRY = handleString(s.getField("BillingCountry")).toString();;
            String BILLINGLATITUDE = handleString(s.getField("BillingLatitude")).toString();
            String BILLINGLONGITUDE = handleString(s.getField(" BillingLongitude")).toString();
            String BILLINGGEOCODEACCURACY =
                handleString(s.getField(" BillingGeocodeAccuracy")).toString();
            String BILLINGADDRESS = handleString(s.getField("BillingAddress")).toString();
            String SHIPPINGSTREET = handleString(s.getField("ShippingStreet")).toString();
            String SHIPPINGCITY = handleString(s.getField("ShippingCity")).toString();
            String SHIPPINGSTATE = handleString(s.getField("ShippingState")).toString();
            String SHIPPINGPOSTALCODE = handleString(s.getField("ShippingPostalCode")).toString();
            String SHIPPINGCOUNTRY = handleString(s.getField("ShippingCountry")).toString();
            String SHIPPINGLATITUDE = handleString(s.getField("ShippingLatitude")).toString();
            String SHIPPINGLONGITUDE = handleString(s.getField("ShippingLongitude")).toString();
            String SHIPPINGGEOCODEACCURACY =
                handleString(s.getField("ShippingGeocodeAccuracy")).toString();
            String SHIPPINGADDRESS = handleString(s.getField("ShippingAddress")).toString();
            String PHONE = handleString(s.getField("Phone")).toString();
            String FAX = handleString(s.getField(" Fax")).toString();
            String ACCOUNTNUMBER = handleString(s.getField(" AccountNumber")).toString();
            String WEBSITE = handleString(s.getField(" Website")).toString();
            String PHOTOURL = handleString(s.getField("PhotoUrl")).toString();
            String INDUSTRY = handleString(s.getField("Industry")).toString();
            String ANNUALREVENUE = handleString(s.getField("AnnualRevenue")).toString();
            String NUMBEROFEMPLOYEES = handleString(s.getField("NumberOfEmployees")).toString();
            String DESCRIPTION = handleString(s.getField("Description")).toString();
            String SITE = handleString(s.getField("Site")).toString();
            String OWNERID = handleString(s.getField("OwnerId")).toString();
            String CREATEDDATE = handleString(s.getField("CreatedDate")).toString();
            String CREATEDBYID = handleString(s.getField("CreatedById")).toString();
            String LASTMODIFIEDDATE = handleString(s.getField("LastModifiedDate")).toString();
            String LASTMODIFIEDBYID = handleString(s.getField("LastModifiedById")).toString();
            String SYSTEMMODSTAMP = handleString(s.getField("SystemModstamp")).toString();
            String LASTACTIVITYDATE = handleString(s.getField("LastActivityDate")).toString();
            String LASTVIEWEDDATE = handleString(s.getField("LastViewedDate")).toString();
            String LASTREFERENCEDDATE = handleString(s.getField("LastReferencedDate")).toString();
            String JIGSAW = handleString(s.getField("Jigsaw")).toString();
            String JIGSAWCOMPANYID = handleString(s.getField("JigsawCompanyId")).toString();
            String ACCOUNTSOURCE = handleString(s.getField("AccountSource")).toString();
            String SICDESC = handleString(s.getField("SicDesc")).toString();
            String REGION__C = handleString(s.getField("Region__c")).toString();
            String ACCOUNT_UNIQUE_KEY__C =
                handleString(s.getField("Account_Unique_Key__c")).toString();
            String PARENT_ACCOUNT_NUMBER__C =
                handleString(s.getField("Parent_Account_Number__c")).toString();
            String ACCOUNT_SHORT_NAME__C =
                handleString(s.getField("Account_Short_Name__c")).toString();
            String PAYMENT_TERMS__C = handleString(s.getField("Payment_Terms__c")).toString();
            String ACTIVE__C = handleString(s.getField("Active__c")).toString();
            String ORACLE_CUSTOMER_ID__C =
                handleString(s.getField("Oracle_Customer_Id__c")).toString();
            String DISTI__C = handleString(s.getField("Disti__c")).toString();
            String APTTUS_CONFIG2__BILLINGCONTACT =
                handleString(s.getField("Apttus_Config2__BillingContactFormat__c")).toString();
            String IHV__C = handleString(s.getField(" IHV__c")).toString();
            String LCM__C = handleString(s.getField("LCM__c")).toString();
            String ODM__C = handleString(s.getField("ODM__c")).toString();
            String OEM__C = handleString(s.getField("OEM__c")).toString();
            String VAR__C = handleString(s.getField("VAR__c")).toString();
            String APTTUS_CONFIG2__BILLINGDAYOFMO =
                handleString(s.getField("Apttus_Config2__BillingDayOfMonth__c")).toString();
            String APTTUS_CONFIG2__BILLINGPREFERE =
                handleString(s.getField("Apttus_Config2__BillingPreferenceId__c")).toString();
            String APTTUS_CONFIG2__CALENDARCYCLES =
                handleString(s.getField("Apttus_Config2__CalendarCycleStart__c")).toString();
            String APTTUS_CONFIG2__CERTIFICATEID_ =
                handleString(s.getField("Apttus_Config2__CertificateId__c")).toString();
            String APTTUS_CONFIG2__CREDITMEMOEMAI =
                handleString(s.getField("Apttus_Config2__CreditMemoEmailTemplate__c")).toString();
            String APTTUS_CONFIG2__DEFAULTCREDITM =
                handleString(s.getField(" Apttus_Config2__DefaultCreditMemoTemplate__c"))
                    .toString();
            String APTTUS_CONFIG2__DEFAULTINVOICE =
                handleString(s.getField("Apttus_Config2__DefaultInvoiceTemplate__c")).toString();
            String APTTUS_CONFIG2__INVOICEEMAILTE =
                handleString(s.getField("Apttus_Config2__InvoiceEmailTemplate__c")).toString();
            String APTTUS_CONFIG2__PAYMENTTERMID_ =
                handleString(s.getField("Apttus_Config2__PaymentTermId__c")).toString();
            String APTTUS_CONFIG2__TAXEXEMPTSTATU =
                handleString(s.getField("Apttus_Config2__TaxExemptStatus__c")).toString();
            String APTTUS_CONFIG2__TAXEXEMPT__C =
                handleString(s.getField(" Apttus_Config2__TaxExempt__c")).toString();
            String BILLINGCON_FIL_CRI =
                handleString(s.getField(" Apttus_Config2__BillingContactFilterCriteria__c"))
                    .toString();
            String DEFAULTINVOICE_STMT_TEMP =
                handleString(s.getField("Apttus_Config2__DefaultInvoiceStatementTemplate__c"))
                    .toString();
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
            preparedStatement1.setString(54, ORACLE_CUSTOMER_ID__C);
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
            preparedStatement1.addBatch();
            count1++;
            if (count1 % batchSize == 0) {
              System.out.println("Commit the batch");
              preparedStatement1.executeBatch();
              connection1.commit();
            }
            System.out.println("Insert into Account ID: " + ID);
          }
          if (qr.isDone()) {
            done = true;
          } else {
            qr = sourceConnection.queryMore(qr.getQueryLocator());
          }
        }
        System.out.println("Final Commit the batch");
        preparedStatement1.executeBatch();
        connection1.commit();
      }
    } catch (Exception e) {
      e.printStackTrace(o);
      result = "Failure";
    } finally {
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace(o);
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace(o);
        }
      }
    }
  }

  @SuppressWarnings("resource")
  private static void queryProduct2() {
    int count1 = 0;
    Connection connection1 = null;
    Connection connection2 = null;
    PreparedStatement preparedStatement1 = null;
    PreparedStatement preparedStatement2 = null;
    ResultSet resultSet1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1.prepareStatement("Truncate Table PRODUCT2_TEMP");
      preparedStatement1.executeUpdate();
      String soqlQuery = "select ID, NAME, PRODUCTCODE, DESCRIPTION, QUANTITYSCHEDULETYPE, "
          + "QUANTITYINSTALLMENTPERIOD, " + "NUMBEROFQUANTITYINSTALLMENTS, REVENUESCHEDULETYPE, "
          + "REVENUEINSTALLMENTPERIOD, NUMBEROFREVENUEINSTALLMENTS, CANUSEQUANTITYSCHEDULE, "
          + "CANUSEREVENUESCHEDULE, ISACTIVE, CREATEDDATE, CREATEDBYID, "
          + "LASTMODIFIEDDATE, LASTMODIFIEDBYID, SYSTEMMODSTAMP, RECORDTYPEID, ISDELETED, "
          + "LASTVIEWEDDATE, LASTREFERENCEDDATE, DIVISION__C, "
          + "PRODUCT_TYPE__C, PRODUCT_GROUP__C, PRODUCT_SERIES__C, "
          + "APTTUS_CONFIG2__BUNDLEINVOICELEVEL__C, "
          + "APTTUS_CONFIG2__CONFIGURATIONTYPE__C, APTTUS_CONFIG2__CUSTOMIZABLE__C, "
          + "APTTUS_CONFIG2__DISCONTINUEDDATE__C, APTTUS_CONFIG2__EFFECTIVEDATE__C, "
          + "APTTUS_CONFIG2__EFFECTIVESTARTDATE__C, APTTUS_CONFIG2__EXCLUDEFROMSITEMAP__C, "
          + "APTTUS_CONFIG2__EXPIRATIONDATE__C, APTTUS_CONFIG2__HASATTRIBUTES__C, "
          + "APTTUS_CONFIG2__HASDEFAULTS__C, APTTUS_CONFIG2__HASOPTIONS__C, "
          + "APTTUS_CONFIG2__HASSEARCHATTRIBUTES__C, APTTUS_CONFIG2__ICONID__C, "
          + "APTTUS_CONFIG2__ICONSIZE__C, "
          + "APTTUS_CONFIG2__ICON__C, APTTUS_CONFIG2__LAUNCHDATE__C, "
          + "APTTUS_CONFIG2__UOM__C, APTTUS_CONFIG2__VERSION__C, "
          + "APTS_COST__C, AGILE_PART_ID__C, LIFECYCLE__C, APTS_IS_DUMMY__C, SYN_PART_TYPE__C, "
          + "PRODUCT_FAMILY_SEGMENT_CODE__C, SYN_EXCLUDE_FROM_PRICING_PRODUCT_SERIES__C FROM Product2";
      QueryResult qr = sourceConnection.query(soqlQuery);
      System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxx Product2 row count " + qr.getSize());
      preparedStatement1 = connection1
          .prepareStatement("INSERT INTO PRODUCT2_TEMP(ID, NAME, PRODUCTCODE, DESCRIPTION, "
              + "QUANTITYSCHEDULETYPE, "
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
      boolean done = false;
      if (qr.getSize() > 0) {
        while (!done) {
          SObject[] records = qr.getRecords();
          for (int i = 0; i < qr.getRecords().length; i++) {
            SObject s = records[i];
            String ID = s.getId();
            String NAME = handleString(s.getField("Name")).toString();
            String PRODUCTCODE = handleString(s.getField("ProductCode")).toString();
            String DESCRIPTION = handleString(s.getField(" Description")).toString();
            String QUANTITYSCHEDULETYPE =
                handleString(s.getField("QuantityScheduleType")).toString();
            String QUANTITYINSTALLMENTPERIOD =
                handleString(s.getField("QuantityInstallmentPeriod")).toString();
            String NUMBEROFQUANTITYINSTALLMENTS =
                handleString(s.getField("NumberOfQuantityInstallments")).toString();
            String REVENUESCHEDULETYPE = handleString(s.getField("RevenueScheduleType")).toString();
            String REVENUEINSTALLMENTPERIOD =
                handleString(s.getField("RevenueInstallmentPeriod")).toString();
            String NUMBEROFREVENUEINSTALLMENTS =
                handleString(s.getField(" NumberOfRevenueInstallments")).toString();
            String CANUSEQUANTITYSCHEDULE =
                handleString(s.getField("CanUseQuantitySchedule")).toString();
            String CANUSEREVENUESCHEDULE =
                handleString(s.getField("CanUseRevenueSchedule")).toString();
            String ISACTIVE = handleString(s.getField("IsActive")).toString();
            String CREATEDDATE = handleString(s.getField("CreatedDate")).toString();
            String CREATEDBYID = handleString(s.getField("CreatedById")).toString();
            String LASTMODIFIEDDATE = handleString(s.getField("LastModifiedDate")).toString();
            String LASTMODIFIEDBYID = handleString(s.getField("LastModifiedById")).toString();
            String SYSTEMMODSTAMP = handleString(s.getField(" SystemModstamp")).toString();
            String RECORDTYPEID = handleString(s.getField("RecordTypeId")).toString();
            String ISDELETED = handleString(s.getField("IsDeleted")).toString();
            String LASTVIEWEDDATE = handleString(s.getField("LastViewedDate")).toString();
            String LASTREFERENCEDDATE = handleString(s.getField("LastReferencedDate")).toString();
            String DIVISION__C = handleString(s.getField("Division__c")).toString();
            String PRODUCT_TYPE__C = handleString(s.getField("Product_Type__c")).toString();
            String PRODUCT_GROUP__C = handleString(s.getField("Product_Group__c")).toString();
            String PRODUCT_SERIES__C = handleString(s.getField("Product_Series__c")).toString();
            String APTTUS_CONFIG2__BUNDLEINVOICEL =
                handleString(s.getField("Apttus_Config2__BundleInvoiceLevel__c")).toString();
            String APTTUS_CONFIG2__CONFIGURATIONT =
                handleString(s.getField("Apttus_Config2__ConfigurationType__c")).toString();
            String APTTUS_CONFIG2__CUSTOMIZABLE__ =
                handleString(s.getField("Apttus_Config2__Customizable__c")).toString();
            String APTTUS_CONFIG2__DISCONTINUEDDA =
                handleString(s.getField("Apttus_Config2__DiscontinuedDate__c")).toString();
            String APTTUS_CONFIG2__EFFECTIVEDATE_ =
                handleString(s.getField("Apttus_Config2__EffectiveDate__c")).toString();
            String APTTUS_CONFIG2__EFFECTIVESTART =
                handleString(s.getField("Apttus_Config2__EffectiveStartDate__c")).toString();
            String APTTUS_CONFIG2__EXCLUDEFROMSIT =
                handleString(s.getField("Apttus_Config2__ExcludeFromSitemap__c")).toString();
            String APTTUS_CONFIG2__EXPIRATIONDATE =
                handleString(s.getField("Apttus_Config2__ExpirationDate__c")).toString();
            String APTTUS_CONFIG2__HASATTRIBUTES_ =
                handleString(s.getField("Apttus_Config2__HasAttributes__c")).toString();
            String APTTUS_CONFIG2__HASDEFAULTS__C =
                handleString(s.getField("Apttus_Config2__HasDefaults__c")).toString();
            String APTTUS_CONFIG2__HASOPTIONS__C =
                handleString(s.getField("Apttus_Config2__HasOptions__c")).toString();
            String APTTUS_CONFIG2__HASSEARCHATTRI =
                handleString(s.getField("Apttus_Config2__HasSearchAttributes__c")).toString();
            String APTTUS_CONFIG2__ICONID__C =
                handleString(s.getField("Apttus_Config2__IconId__c")).toString();
            String APTTUS_CONFIG2__ICONSIZE__C =
                handleString(s.getField("Apttus_Config2__IconSize__c")).toString();
            String APTTUS_CONFIG2__ICON__C = null;
            String APTTUS_CONFIG2__LAUNCHDATE__C =
                handleString(s.getField("Apttus_Config2__LaunchDate__c")).toString();
            String APTTUS_CONFIG2__UOM__C =
                handleString(s.getField("Apttus_Config2__Uom__c")).toString();
            String APTTUS_CONFIG2__VERSION__C =
                handleString(s.getField("Apttus_Config2__Version__c")).toString();
            String APTS_COST__C = handleString(s.getField("APTS_Cost__c")).toString();
            String AGILE_PART_ID__C = handleString(s.getField("Agile_Part_ID__c")).toString();
            String LIFECYCLE__C = handleString(s.getField("Lifecycle__c")).toString();
            String APTS_IS_DUMMY__C = handleString(s.getField("APTS_Is_Dummy__c")).toString();
            String SYN_PART_TYPE__C = handleString(s.getField("SYN_Part_Type__c")).toString();
            String PRODUCT_FAMILY_SEGMENT_CODE__C =
                handleString(s.getField("Product_Family_Segment_Code__c")).toString();
            String SYN_EXCLUDE_FROM_PRICING_PRODU =
                handleString(s.getField("SYN_Exclude_from_Pricing_Product_Series__c")).toString();
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
            preparedStatement1.setString(14, CREATEDDATE);
            preparedStatement1.setString(15, CREATEDBYID);
            preparedStatement1.setString(16, LASTMODIFIEDDATE);
            preparedStatement1.setString(17, LASTMODIFIEDBYID);
            preparedStatement1.setString(18, SYSTEMMODSTAMP);
            preparedStatement1.setString(19, RECORDTYPEID);
            preparedStatement1.setString(20, ISDELETED);
            preparedStatement1.setString(21, LASTVIEWEDDATE);
            preparedStatement1.setString(22, LASTREFERENCEDDATE);
            preparedStatement1.setString(23, DIVISION__C);
            preparedStatement1.setString(24, PRODUCT_TYPE__C);
            preparedStatement1.setString(25, PRODUCT_GROUP__C);
            preparedStatement1.setString(26, PRODUCT_SERIES__C);
            preparedStatement1.setString(27, APTTUS_CONFIG2__BUNDLEINVOICEL);
            preparedStatement1.setString(28, APTTUS_CONFIG2__CONFIGURATIONT);
            preparedStatement1.setString(29, APTTUS_CONFIG2__CUSTOMIZABLE__);
            preparedStatement1.setString(30, APTTUS_CONFIG2__DISCONTINUEDDA);
            preparedStatement1.setString(31, APTTUS_CONFIG2__EFFECTIVEDATE_);
            preparedStatement1.setString(32, APTTUS_CONFIG2__EFFECTIVESTART);
            preparedStatement1.setString(33, APTTUS_CONFIG2__EXCLUDEFROMSIT);
            preparedStatement1.setString(34, APTTUS_CONFIG2__EXPIRATIONDATE);
            preparedStatement1.setString(35, APTTUS_CONFIG2__HASATTRIBUTES_);
            preparedStatement1.setString(36, APTTUS_CONFIG2__HASDEFAULTS__C);
            preparedStatement1.setString(37, APTTUS_CONFIG2__HASOPTIONS__C);
            preparedStatement1.setString(38, APTTUS_CONFIG2__HASSEARCHATTRI);
            preparedStatement1.setString(39, APTTUS_CONFIG2__ICONID__C);
            preparedStatement1.setString(40, APTTUS_CONFIG2__ICONSIZE__C);
            preparedStatement1.setString(41, APTTUS_CONFIG2__ICON__C);
            preparedStatement1.setString(42, APTTUS_CONFIG2__LAUNCHDATE__C);
            preparedStatement1.setString(43, APTTUS_CONFIG2__UOM__C);
            preparedStatement1.setString(44, APTTUS_CONFIG2__VERSION__C);
            preparedStatement1.setString(45, APTS_COST__C);
            preparedStatement1.setString(46, AGILE_PART_ID__C);
            preparedStatement1.setString(47, LIFECYCLE__C);
            preparedStatement1.setString(48, APTS_IS_DUMMY__C);
            preparedStatement1.setString(49, SYN_PART_TYPE__C);
            preparedStatement1.setString(50, PRODUCT_FAMILY_SEGMENT_CODE__C);
            preparedStatement1.setString(51, SYN_EXCLUDE_FROM_PRICING_PRODU);
            preparedStatement1.addBatch();
            count1++;
            if (count1 % batchSize == 0) {
              System.out.println("Commit the batch");
              preparedStatement1.executeBatch();
              connection1.commit();
            }
            System.out.println("Inserting into Product2 Id: " + ID);
          }
          if (qr.isDone()) {
            done = true;
          } else {
            qr = sourceConnection.queryMore(qr.getQueryLocator());
          }
        }
        System.out.println("Final Commit the batch");
        preparedStatement1.executeBatch();
        connection1.commit();
      }
    } catch (Exception e) {
      e.printStackTrace(o);
      result = "Failure";
    } finally {
      if (preparedStatement2 != null) {
        try {
          preparedStatement2.close();
        } catch (SQLException e) {
          e.printStackTrace(o);
        }
      }
      if (resultSet1 != null) {
        try {
          resultSet1.close();
        } catch (SQLException e) {
          e.printStackTrace(o);
        }
      }
      if (connection2 != null) {
        try {
          connection2.close();
        } catch (SQLException e) {
          e.printStackTrace(o);
        }
      }
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace(o);
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace(o);
        }
      }
    }
  }

  @SuppressWarnings("resource")
  private static void queryOpportunity() {
    int count1 = 0;
    Connection connection1 = null;
    Connection connection2 = null;
    PreparedStatement preparedStatement1 = null;
    PreparedStatement preparedStatement2 = null;
    ResultSet resultSet1 = null;
    try {
      connection1 = getConnection();
      preparedStatement1 = connection1.prepareStatement("Truncate Table OPPORTUNITY_TEMP");
      preparedStatement1.executeUpdate();
      String soqlQuery = "select ACCOUNTID, "
          + " ACTUALS_AMOUNT__C, ACTUAL_DESIGN_IN_DATE__C, ACTUAL_SYNA_MP_DATE__C, "
          + "AMOUNT, APPLICATION__C, APTS_ALLOW_OPPORTUNITY_STAGE_CHANGE__C, APTS_PROJECT_TYPE__C, "
          + "APTTUS_APPROVAL__APPROVAL_STATUS__C, APTTUS_PROPOSAL__DISCOUNT_PERCENT__C, ASP__C, "
          + "AUDIT_ACTUAL_DW_VS_CLOSEDATE__C, AVERAGE_MONTHLY_QUANTITY__C, "
          + "BUDGETARY_PRICE_APPROVAL_DATE__C, BUDGETARY__C, BU_PRICE_APPROVED_DATE__C, "
          + "CAMPAIGNID, CHANNEL__C, CLEARPAD_SERIES__C, CLOSEDATE, CLOSED_DATE_PAST_DUE__C, "
          + "COUNT_OF_BU_REQUESTED_TIMESTAMPED__C, CREATEDBYID, CREATEDDATE, "
          + "CURRENT_SALES_NET_PRICE__C, CUSTOMER_ID__C, CUSTOMER_PART_NUMBER__C, "
          + "CUSTOMER_REQUIREMENTS__C, CUSTOMER_SPECIFIC_QUOTE_CONDITIONS__C, "
          + "DAYS_SINCE_LAST_MODIFIED_BY_USER__C, DAYS_TO_CLOSE_DATE__C, "
          + "DAYS_TO_DESIGN_IN__C, DESCRIPTION, DESIGN_IN_DATE__C, "
          + "DISPLAY_SIZE_TYPE__C, DIVISION__C, EARLY_SAMPLE_DATE__C, ENGAGEMENT_MODEL__C, "
          + "ESTIMATED_REVENUE__C, EXPECTED_DESIGN_WIN_DATE__C, EXPECTED_RFI_RFQ_DATE__C, "
          + " FIRST_CUSTOMER_PO_NUMBER__C, FORECASTCATEGORY, FORECASTCATEGORYNAME, "
          + "FORECAST_QUOTE_PRIMARY_PRODUCT_NAME__C, FORECAST_QUOTE__C, FY16_QUOTE__C, "
          + "GESTURES_CAPABILITIES__C, GESTURE_SUPPORT__C, HALOGEN_FREE_SUPPORT__C, "
          + "HASOPENACTIVITY, HASOPPORTUNITYLINEITEM, HASOVERDUETASK, ID, INTERFACE__C, "
          + "ISCLOSED, ISDELETED, ISPRIVATE, ISWON, JIRA_NUMBER__C, LASTACTIVITYDATE, "
          + "LASTMODIFIEDBYID, LASTMODIFIEDDATE, LASTREFERENCEDDATE, LASTVIEWEDDATE, "
          + "LAST_UPDATED_BY_USER__C, LCM_OPPORTUNITY_LOOKUP__C, LCM_REFERENCE_ACCOUNT__C, "
          + "LCM_TO_OEM_REFERENCE_ACCOUNT__C,LCM_TO_OEM_REFERENCE_OPPORTUNITY__C, LEADSOURCE, "
          + "LIFETIME_QUANTITY_TIER__C, LIFETIME_QUANTITY__C, MARKET_SEGMENT_CODE__C, "
          + "MOBILE_OS__C, MODEL_NAME_IN_MARKET__C, MOQ__C, MP_DATE__C, "
          + "MP_FORECAST_FLAG__C, NAME, NEXTSTEP, NOTEBOOK_LCD_SIZE__C, "
          + "NUMBER_OF_COMPETITORS__C, NUMBER_OF_DAYS_PAST_DUE__C, "
          + "NUMBER_OF_MONTHS_LIFETIME__C, NUMBER_OF_OPPORTUNITIES__C, "
          + "NUMBER_OF_PRODUCTS__C,  NUMBER_OF_QUOTES_WITH_PRODUCTS__C, NUMBER_OF_QUOTES__C, "
          + "ODM_PROGRAM_NAME__C, ODM_REFERENCE_ACCOUNT__C, ODM_SUGGESTED_FSF__C, "
          + "OEM_OPPORTUNITY_LOOKUP__C, OEM_OPPORTUNITY_NAME__C, OEM_PROGRAM_NAME_INPUT__C, "
          + "OEM_REFERENCE_ACCOUNT_ID__C, OEM_REFERENCE_ACCOUNT__C, OEM_SUGGESTED_FSF__C, "
          + "OEM_VAR_ACCOUNT__C, OF_ADDITIONAL_QUOTES__C, OF_RELATED_ODM_S__C, OID__C, "
          + "OMIT_FROM_FORECAST__C, OPA_SAME_ACCOUNT_ROLLUP__C, OPPORTUNITY_FORECAST__C, "
          + " OPPORTUNITY_LIFETIME_QUANTITY__C, OPPORTUNITY_OWNER_EMP_ID__C, "
          + "OPPORTUNITY_REPORT_NAME__C, OPPORTUNITY_STAGE_SCORE__C, OPPORTUNITY_VOLUME__C, "
          + "ORACLE_SALES_ORDER_NUMBER__C, ORIGINAL_PART_NUMBER__C, OS__C, "
          + "OTHER_INTERFACE_DETAILS__C, OWNERID, PANEL_RESOLUTION__C, PANEL_SIZE__C, "
          + "PARENT_OEM_ACCOUNT__C, PHONE_TYPE__C, PLANNING_POLICY__C, PRECISION_TOUCHPAD__C, "
          + "PREFERRED_SUPPLIER_LENS__C, PREFERRED_SUPPLIER_SENSOR__C, PRICEBOOK2ID, "
          + "PRICE_REDUCTION_CONSOLE__C, PRIMARY_APPROVED__C, PRIMARY_CONTACT__C, "
          + "PRIMARY_PRODUCT_STATUS__C, PRIMARY_QUOTE_BILL_TO_NAME__C, PRIMARY_SOLUTION__C, "
          + "PROBABILITY, PRODUCT_GROUP__C, PRODUCT_TYPE2__C, PRODUCT__C, "
          + "PROJECT_ACCOUNT_NAME_S_REGION__C,PROJECT_MODEL_NAME_IN_MARKET__C, "
          + "PROJECT_VOLUME__C, PROJECT__C, PURCHASE_ORDER_DATE__C, QUANTITY_PER_MONTH__C, "
          + "QUOTE_BILL_TO_PAYMENT_TERMS__C, QUOTE_BILL_TO__C, QUOTE_CONDITIONS__C, "
          + "QUOTE_SENT_TO_CUSTOMER__C, QUOTE_STAGE_VALUE_ROLLUP__C, RECORDTYPEID, "
          + "REP__C, REQUEST_FOR_BUDGETARY_QUOTE__C, REQUEST_FOR_RE_QUOTE__C, "
          + "RE_QUOTE__C, RFQ__C, ROLLUPCOMPETITORS__C, ROLLUP_ACTUAL_DESIGN_WIN_DATE__C, "
          + "RUNNING_USER__C, SALES_REGION__C, SEGMENTATION__C, SFID__C, SHIPPING_TERMS__C, "
          + "SINGLE_MULTI_SOURCE__C, SOW_SENT_TO_BU__C, STAGENAME, STANDARDIZED_OPPORTUNITY_NAME__C, "
          + "STRATEGIC__C, SUBMIT_FOR_QUOTE__C, SYNAPTICS_SHARE_PERCENTAGE__C, "
          + "SYNA_PART_NUMBER__C, SYNA_SHARE__C, SYNCEDQUOTEID, SYSTEMMODSTAMP, "
          + "TOTAL_CUSTOMER_PROJECT_QTY__C,TOTAL_OPPORTUNITY_FAE_INVOLVEMENT__C, TYPE, "
          + "WHITEBOX__C, WIN_RATE__C, STANDARDIZED_OPPORTUNITY_NAME_INDEXED__C FROM Opportunity";
      QueryResult qr = sourceConnection.query(soqlQuery);
      System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxx Oppurtunity row count " + qr.getSize());
      preparedStatement1 = connection1.prepareStatement(
          "INSERT INTO OPPORTUNITY_TEMP(ACCOUNTID, ACTUALS_AMOUNT__C, ACTUAL_DESIGN_IN_DATE__C, "
              + "ACTUAL_SYNA_MP_DATE__C, AMOUNT, APPLICATION__C, APTS_ALLOW_OPPORTUNITY_STAGE_C, "
              + "APTS_PROJECT_TYPE__C, APTTUS_APPROVAL__APPROVAL_STAT,"
              + " APTTUS_PROPOSAL__DISCOUNT_PERC, "
              + "ASP__C, AUDIT_ACTUAL_DW_VS_CLOSEDATE__, AVERAGE_MONTHLY_QUANTITY__C, "
              + "BU_PRICE_APPROVED_DATE__C, BUDGETARY__C, BUDGETARY_PRICE_APPROVAL_DATE_, "
              + "CAMPAIGNID, CHANNEL__C, CLEARPAD_SERIES__C, CLOSEDATE, CLOSED_DATE_PAST_DUE__C, "
              + "COUNT_OF_BU_REQUESTED_TIMESTAM, CREATEDBYID, CREATEDDATE, CURRENT_SALES_NET_PRICE__C, "
              + "CUSTOMER_ID__C, CUSTOMER_PART_NUMBER__C, CUSTOMER_REQUIREMENTS__C, "
              + "CUSTOMER_SPECIFIC_QUOTE_CONDIT, DAYS_SINCE_LAST_MODIFIED_BY_US, "
              + "DAYS_TO_CLOSE_DATE__C, "
              + "DAYS_TO_DESIGN_IN__C, DESCRIPTION, DESIGN_IN_DATE__C, DISPLAY_SIZE_TYPE__C, "
              + "DIVISION__C, EARLY_SAMPLE_DATE__C, ENGAGEMENT_MODEL__C, ESTIMATED_REVENUE__C, "
              + "EXPECTED_DESIGN_WIN_DATE__C, EXPECTED_RFI_RFQ_DATE__C,"
              + " FIRST_CUSTOMER_PO_NUMBER__C, "
              + "FORECASTCATEGORY, FORECASTCATEGORYNAME, FORECAST_QUOTE_PRIMARY_PRODUCT, "
              + "FORECAST_QUOTE__C, FY16_QUOTE__C, GESTURES_CAPABILITIES__C, GESTURE_SUPPORT__C, "
              + "HALOGEN_FREE_SUPPORT__C, HASOPENACTIVITY, HASOPPORTUNITYLINEITEM, HASOVERDUETASK, "
              + "ID, INTERFACE__C, ISCLOSED, ISDELETED, ISPRIVATE, ISWON, JIRA_NUMBER__C, "
              + "LASTACTIVITYDATE, LASTMODIFIEDBYID, LASTMODIFIEDDATE, LASTREFERENCEDDATE, "
              + "LASTVIEWEDDATE, LAST_UPDATED_BY_USER__C, LCM_OPPORTUNITY_LOOKUP__C, "
              + "LCM_REFERENCE_ACCOUNT__C, LCM_TO_OEM_REFERENCE_ACCOUNT__, LCM_TO_OEM_REFERENCE_OPPORTUNI, "
              + "LEADSOURCE, LIFETIME_QUANTITY_TIER__C, LIFETIME_QUANTITY__C, "
              + "MARKET_SEGMENT_CODE__C, "
              + "MOBILE_OS__C, MODEL_NAME_IN_MARKET__C, MOQ__C, MP_DATE__C, MP_FORECAST_FLAG__C, "
              + "NAME, NEXTSTEP, NOTEBOOK_LCD_SIZE__C, NUMBER_OF_COMPETITORS__C, "
              + "NUMBER_OF_DAYS_PAST_DUE__C, NUMBER_OF_MONTHS_LIFETIME__C, "
              + "NUMBER_OF_OPPORTUNITIES__C, "
              + "NUMBER_OF_PRODUCTS__C, NUMBER_OF_QUOTES_WITH_PRODUCTS, NUMBER_OF_QUOTES__C, "
              + "ODM_PROGRAM_NAME__C, ODM_REFERENCE_ACCOUNT__C, ODM_SUGGESTED_FSF__C, "
              + "OEM_OPPORTUNITY_LOOKUP__C, OEM_OPPORTUNITY_NAME__C, OEM_PROGRAM_NAME_INPUT__C, "
              + "OEM_REFERENCE_ACCOUNT_ID__C, OEM_REFERENCE_ACCOUNT__C, OEM_SUGGESTED_FSF__C, "
              + "OEM_VAR_ACCOUNT__C, OF_ADDITIONAL_QUOTES__C, OF_RELATED_ODM_S__C, OID__C, "
              + "OMIT_FROM_FORECAST__C, OPA_SAME_ACCOUNT_ROLLUP__C, OPPORTUNITY_FORECAST__C, "
              + "OPPORTUNITY_LIFETIME_QUANTITY_, OPPORTUNITY_OWNER_EMP_ID__C, "
              + "OPPORTUNITY_REPORT_NAME__C, "
              + "OPPORTUNITY_STAGE_SCORE__C, OPPORTUNITY_VOLUME__C, ORACLE_SALES_ORDER_NUMBER__C, "
              + "ORIGINAL_PART_NUMBER__C, OS__C, OTHER_INTERFACE_DETAILS__C, OWNERID, "
              + "PANEL_RESOLUTION__C, PANEL_SIZE__C, PARENT_OEM_ACCOUNT__C, PHONE_TYPE__C, "
              + "PLANNING_POLICY__C, PRECISION_TOUCHPAD__C, PREFERRED_SUPPLIER_LENS__C, "
              + "PREFERRED_SUPPLIER_SENSOR__C, PRICEBOOK2ID, PRICE_REDUCTION_CONSOLE__C, "
              + "PRIMARY_APPROVED__C, PRIMARY_CONTACT__C, PRIMARY_PRODUCT_STATUS__C, "
              + "PRIMARY_QUOTE_BILL_TO_NAME__C, PRIMARY_SOLUTION__C, PROBABILITY, "
              + "PRODUCT_GROUP__C, "
              + "PRODUCT_TYPE2__C, PRODUCT__C, PROJECT_ACCOUNT_NAME_S_REGION_, "
              + "PROJECT_MODEL_NAME_IN_MARKET__, "
              + "PROJECT_VOLUME__C, PROJECT__C, PURCHASE_ORDER_DATE__C, QUANTITY_PER_MONTH__C, "
              + "QUOTE_BILL_TO_PAYMENT_TERMS__C, QUOTE_BILL_TO__C, QUOTE_CONDITIONS__C, "
              + "QUOTE_SENT_TO_CUSTOMER__C, QUOTE_STAGE_VALUE_ROLLUP__C, RECORDTYPEID, REP__C, "
              + "REQUEST_FOR_BUDGETARY_QUOTE__C, REQUEST_FOR_RE_QUOTE__C, RE_QUOTE__C, RFQ__C, "
              + "ROLLUPCOMPETITORS__C, ROLLUP_ACTUAL_DESIGN_WIN_DATE_, RUNNING_USER__C, "
              + "SALES_REGION__C, "
              + "SEGMENTATION__C, SFID__C, SHIPPING_TERMS__C, SINGLE_MULTI_SOURCE__C, "
              + "SOW_SENT_TO_BU__C, STAGENAME, STANDARDIZED_OPPORTUNITY_NAME_, STRATEGIC__C, "
              + "SUBMIT_FOR_QUOTE__C, SYNAPTICS_SHARE_PERCENTAGE__C, SYNA_PART_NUMBER__C, "
              + "SYNA_SHARE__C, SYNCEDQUOTEID, SYSTEMMODSTAMP, TOTAL_CUSTOMER_PROJECT_QTY__C, "
              + "TOTAL_OPPORTUNITY_FAE_INVOLVEM, TYPE, WHITEBOX__C, WIN_RATE__C,FORECAST_QU_PRI_PRO_NAME_C,STAND_OPPO_NAME_INDEXED_C)"
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
              + " ?, ?, ?, " + "?, ?, ?, ?, ?, ?, ?, ?, ?,?)");
      boolean done = false;
      if (qr.getSize() > 0) {
        while (!done) {
          SObject[] records = qr.getRecords();
          for (int i = 0; i < qr.getRecords().length; i++) {
            SObject s = records[i];
            String ACCOUNTID = handleString(s.getField("AccountId")).toString();
            String ACTUALS_AMOUNT__C = handleString(s.getField("Actuals_Amount__c")).toString();
            String ACTUAL_DESIGN_IN_DATE__C =
                handleString(s.getField("Actual_Design_In_Date__c")).toString();
            String ACTUAL_SYNA_MP_String__C =
                handleString(s.getField("Actual_SYNA_MP_Date__c")).toString();
            String AMOUNT = handleString(s.getField("Amount")).toString();
            String APPLICATION__C = handleString(s.getField("Application__c")).toString();
            String APTS_ALLOW_OPPORTUNITY_STAGE_C =
                handleString(s.getField("APTS_Allow_Opportunity_Stage_Change__c")).toString();
            String APTS_PROJECT_TYPE__C =
                handleString(s.getField("APTS_Project_Type__c")).toString();
            String APTTUS_APPROVAL__APPROVAL_STAT =
                handleString(s.getField("Apttus_Approval__Approval_Status__c")).toString();
            String APTTUS_PROPOSAL__DISCOUNT_PERC =
                handleString(s.getField("Apttus_Proposal__Discount_Percent__c")).toString();
            String ASP__C = handleString(s.getField("ASP__c")).toString();
            String AUDIT_ACTUAL_DW_VS_CLOSEString__ =
                handleString(s.getField("Audit_Actual_DW_vs_CloseDate__c")).toString();
            String AVERAGE_MONTHLY_QUANTITY__C =
                handleString(s.getField("Average_Monthly_Quantity__c")).toString();
            String BUDGETARY_PRICE_APPROVAL_String_ =
                handleString(s.getField("Budgetary_Price_Approval_Date__c")).toString();
            String BUDGETARY__C = handleString(s.getField("Budgetary__c")).toString();
            String BU_PRICE_APPROVED_String__C =
                handleString(s.getField("BU_Price_Approved_Date__c")).toString();
            String CAMPAIGNID = handleString(s.getField("CampaignId")).toString();
            String CHANNEL__C = handleString(s.getField("Channel__c")).toString();
            String CLEARPAD_SERIES__C = handleString(s.getField("ClearPad_Series__c")).toString();
            String CLOSEString = handleString(s.getField("CloseDate")).toString();
            String CLOSED_String_PAST_DUE__C =
                handleString(s.getField("Closed_Date_Past_Due__c")).toString();
            String COUNT_OF_BU_REQUESTED_TIMESTAM =
                handleString(s.getField("Count_of_BU_Requested_Timestamped__c")).toString();
            String CREATEDBYID = handleString(s.getField("CreatedById")).toString();
            String CREATEDString = handleString(s.getField("CreatedDate")).toString();
            String CURRENT_SALES_NET_PRICE__C =
                handleString(s.getField("Current_Sales_Net_Price__c")).toString();
            String CUSTOMER_ID__C = handleString(s.getField("Customer_Id__c")).toString();
            String CUSTOMER_PART_NUMBER__C =
                handleString(s.getField("Customer_Part_Number__c")).toString();
            String CUSTOMER_REQUIREMENTS__C =
                handleString(s.getField("Customer_Requirements__c")).toString();
            String CUSTOMER_SPECIFIC_QUOTE_CONDIT =
                handleString(s.getField("Customer_Specific_Quote_Conditions__c")).toString();
            String DAYS_SINCE_LAST_MODIFIED_BY_US =
                handleString(s.getField("Days_Since_Last_Modified_by_User__c")).toString();
            String DAYS_TO_CLOSE_String__C =
                handleString(s.getField("Days_to_Close_Date__c")).toString();
            String DAYS_TO_DESIGN_IN__C =
                handleString(s.getField("Days_to_Design_In__c")).toString();
            String DESCRIPTION = handleString(s.getField("Description")).toString();
            String DESIGN_IN_String__C = handleString(s.getField("Design_In_Date__c")).toString();
            String DISPLAY_SIZE_TYPE__C =
                handleString(s.getField("Display_Size_Type__c")).toString();
            String DIVISION__C = handleString(s.getField("Division__c")).toString();
            String EARLY_SAMPLE_String__C =
                handleString(s.getField("Early_Sample_Date__c")).toString();
            String ENGAGEMENT_MODEL__C = handleString(s.getField("Engagement_Model__c")).toString();
            String ESTIMATED_REVENUE__C =
                handleString(s.getField("Estimated_Revenue__c")).toString();
            String EXPECTED_DESIGN_WIN_String__C =
                handleString(s.getField("Expected_Design_Win_Date__c")).toString();
            String EXPECTED_RFI_RFQ_String__C =
                handleString(s.getField("Expected_RFI_RFQ_Date__c")).toString();
            String FIRST_CUSTOMER_PO_NUMBER__C =
                handleString(s.getField("First_Customer_PO_Number__c")).toString();
            String FORECASTCATEGORY = handleString(s.getField("ForecastCategory")).toString();
            String FORECASTCATEGORYNAME =
                handleString(s.getField("ForecastCategoryName")).toString();
            String FORECAST_QUOTE_PRIMARY_PRODUCT =
                handleString(s.getField("Forecast_Quote_Primary_Product_Name__c")).toString();
            String FORECAST_QUOTE__C = handleString(s.getField("Forecast_Quote__c")).toString();
            String FY16_QUOTE__C = handleString(s.getField("FY16_Quote__c")).toString();
            String GESTURES_CAPABILITIES__C =
                handleString(s.getField("Gestures_Capabilities__c")).toString();
            String GESTURE_SUPPORT__C = handleString(s.getField("Gesture_Support__c")).toString();
            String HALOGEN_FREE_SUPPORT__C =
                handleString(s.getField("Halogen_Free_Support__c")).toString();
            String HASOPENACTIVITY = handleString(s.getField("HasOpenActivity")).toString();
            String HASOPPORTUNITYLINEITEM =
                handleString(s.getField("HasOpportunityLineItem")).toString();
            String HASOVERDUETASK = handleString(s.getField("HasOverdueTask")).toString();
            String ID = s.getId();
            String INTERFACE__C = handleString(s.getField("Interface__c")).toString();
            String ISCLOSED = handleString(s.getField("IsClosed")).toString();
            String ISDELETED = handleString(s.getField("IsDeleted")).toString();
            String ISPRIVATE = handleString(s.getField("IsPrivate")).toString();
            String ISWON = handleString(s.getField("IsWon")).toString();
            String JIRA_NUMBER__C = handleString(s.getField("JIRA_Number__c")).toString();
            String LASTACTIVITYString = handleString(s.getField("LastActivityDate")).toString();
            String LASTMODIFIEDBYID = handleString(s.getField("LastModifiedById")).toString();
            String LASTMODIFIEDString = handleString(s.getField("LastModifiedDate")).toString();
            String LASTREFERENCEDString = handleString(s.getField("LastReferencedDate")).toString();
            String LASTVIEWEDString = handleString(s.getField("LastViewedDate")).toString();
            String LAST_UPStringD_BY_USER__C =
                handleString(s.getField("Last_Updated_by_User__c")).toString();
            String LCM_OPPORTUNITY_LOOKUP__C =
                handleString(s.getField("LCM_Opportunity_Lookup__c")).toString();
            String LCM_REFERENCE_ACCOUNT__C =
                handleString(s.getField("LCM_Reference_Account__c")).toString();
            String LCM_TO_OEM_REFERENCE_ACCOUNT__ =
                handleString(s.getField("LCM_to_OEM_Reference_Account__c")).toString();
            String LCM_TO_OEM_REFERENCE_OPPORTUNI =
                handleString(s.getField("LCM_to_OEM_Reference_Opportunity__c")).toString();
            String LEADSOURCE = handleString(s.getField("LeadSource")).toString();
            String LIFETIME_QUANTITY_TIER__C =
                handleString(s.getField("Lifetime_Quantity_Tier__c")).toString();
            String LIFETIME_QUANTITY__C =
                handleString(s.getField("Lifetime_Quantity__c")).toString();
            String MARKET_SEGMENT_CODE__C =
                handleString(s.getField("Market_Segment_Code__c")).toString();
            String MOBILE_OS__C = handleString(s.getField("Mobile_OS__c")).toString();
            String MODEL_NAME_IN_MARKET__C =
                handleString(s.getField("Model_Name_in_Market__c")).toString();
            String MOQ__C = handleString(s.getField("MOQ__c")).toString();
            String MP_String__C = handleString(s.getField("MP_Date__c")).toString();
            String MP_FORECAST_FLAG__C = handleString(s.getField("MP_Forecast_Flag__c")).toString();
            String NAME = handleString(s.getField("Name")).toString();
            String NEXTSTEP = handleString(s.getField("NextStep")).toString();
            String NOTEBOOK_LCD_SIZE__C =
                handleString(s.getField("Notebook_LCD_Size__c")).toString();
            String NUMBER_OF_COMPETITORS__C =
                handleString(s.getField("Number_of_Competitors__c")).toString();
            String NUMBER_OF_DAYS_PAST_DUE__C =
                handleString(s.getField("Number_of_Days_Past_Due__c")).toString();
            String NUMBER_OF_MONTHS_LIFETIME__C =
                handleString(s.getField("Number_of_Months_Lifetime__c")).toString();
            String NUMBER_OF_OPPORTUNITIES__C =
                handleString(s.getField("Number_of_Opportunities__c")).toString();
            String NUMBER_OF_PRODUCTS__C =
                handleString(s.getField("Number_of_Products__c")).toString();
            String NUMBER_OF_QUOTES_WITH_PRODUCTS =
                handleString(s.getField("Number_of_Quotes_with_Products__c")).toString();
            String NUMBER_OF_QUOTES__C = handleString(s.getField("Number_of_Quotes__c")).toString();
            String ODM_PROGRAM_NAME__C = handleString(s.getField("ODM_Program_Name__c")).toString();
            String ODM_REFERENCE_ACCOUNT__C =
                handleString(s.getField("OEM_Reference_Account__c")).toString();
            String ODM_SUGGESTED_FSF__C =
                handleString(s.getField("ODM_Suggested_FSF__c")).toString();
            String OEM_OPPORTUNITY_LOOKUP__C =
                handleString(s.getField("OEM_Opportunity_lookup__c")).toString();
            String OEM_OPPORTUNITY_NAME__C =
                handleString(s.getField("OEM_Opportunity_Name__c")).toString();
            String OEM_PROGRAM_NAME_INPUT__C =
                handleString(s.getField("OEM_Program_Name_Input__c")).toString();
            String OEM_REFERENCE_ACCOUNT_ID__C =
                handleString(s.getField("OEM_Reference_Account_ID__c")).toString();
            String OEM_REFERENCE_ACCOUNT__C =
                handleString(s.getField("OEM_Reference_Account__c")).toString();
            String OEM_SUGGESTED_FSF__C =
                handleString(s.getField("ODM_Suggested_FSF__c")).toString();
            String OEM_VAR_ACCOUNT__C = handleString(s.getField("OEM_VAR_Account__c")).toString();
            String OF_ADDITIONAL_QUOTES__C =
                handleString(s.getField("of_Additional_Quotes__c")).toString();
            String OF_RELATED_ODM_S__C = handleString(s.getField("of_related_ODM_s__c")).toString();
            String OID__C = handleString(s.getField("OID__c")).toString();
            String OMIT_FROM_FORECAST__C =
                handleString(s.getField("Omit_From_Forecast__c")).toString();
            String OPA_SAME_ACCOUNT_ROLLUP__C =
                handleString(s.getField("OPA_same_account_rollup__c")).toString();
            String OPPORTUNITY_FORECAST__C = null;
            String OPPORTUNITY_LIFETIME_QUANTITY_ =
                handleString(s.getField("Opportunity_Lifetime_Quantity__c")).toString();
            String OPPORTUNITY_OWNER_EMP_ID__C =
                handleString(s.getField("Opportunity_Owner_Emp_ID__c")).toString();
            String OPPORTUNITY_REPORT_NAME__C =
                handleString(s.getField("Opportunity_Report_Name__c")).toString();
            String OPPORTUNITY_STAGE_SCORE__C =
                handleString(s.getField("Opportunity_Stage_Score__c")).toString();
            String OPPORTUNITY_VOLUME__C =
                handleString(s.getField("Opportunity_Volume__c")).toString();
            String ORACLE_SALES_ORDER_NUMBER__C =
                handleString(s.getField("Oracle_Sales_Order_Number__c")).toString();
            String ORIGINAL_PART_NUMBER__C =
                handleString(s.getField("Original_Part_Number__c")).toString();
            String OS__C = handleString(s.getField("OS__c")).toString();
            String OTHER_INTERFACE_DETAILS__C =
                handleString(s.getField("Other_Interface_Details__c")).toString();
            String OWNERID = handleString(s.getField("OwnerId")).toString();
            String PANEL_RESOLUTION__C = handleString(s.getField("Panel_Resolution__c")).toString();
            String PANEL_SIZE__C = handleString(s.getField("Panel_Size__c")).toString();
            String PARENT_OEM_ACCOUNT__C =
                handleString(s.getField("Parent_OEM_Account__c")).toString();
            String PHONE_TYPE__C = handleString(s.getField("Phone_Type__c")).toString();
            String PLANNING_POLICY__C = handleString(s.getField("Planning_Policy__c")).toString();
            String PRECISION_TOUCHPAD__C =
                handleString(s.getField("Precision_TouchPad__c")).toString();
            String PREFERRED_SUPPLIER_LENS__C =
                handleString(s.getField("Preferred_Supplier_Lens__c")).toString();
            String PREFERRED_SUPPLIER_SENSOR__C =
                handleString(s.getField("Preferred_Supplier_Sensor__c")).toString();
            String PRICEBOOK2ID = handleString(s.getField("Pricebook2Id")).toString();
            String PRICE_REDUCTION_CONSOLE__C = null;
            String PRIMARY_APPROVED__C = handleString(s.getField("Primary_Approved__c")).toString();
            String PRIMARY_CONTACT__C = handleString(s.getField("Primary_Contact__c")).toString();
            String PRIMARY_PRODUCT_STATUS__C =
                handleString(s.getField("Primary_Product_Status__c")).toString();
            String PRIMARY_QUOTE_BILL_TO_NAME__C =
                handleString(s.getField("Primary_Quote_Bill_To_Name__c")).toString();
            String PRIMARY_SOLUTION__C = handleString(s.getField("Primary_Solution__c")).toString();
            String PROBABILITY = handleString(s.getField("Probability")).toString();
            String PRODUCT_GROUP__C = handleString(s.getField("Product_Group__c")).toString();
            String PRODUCT_TYPE2__C = handleString(s.getField("Product_Type2__c")).toString();
            String PRODUCT__C = handleString(s.getField("Product__c")).toString();
            String PROJECT_ACCOUNT_NAME_S_REGION_ =
                handleString(s.getField("Project_Account_Name_s_Region__c")).toString();
            String PROJECT_MODEL_NAME_IN_MARKET__ =
                handleString(s.getField("Project_Model_Name_in_Market__c")).toString();
            String PROJECT_VOLUME__C = handleString(s.getField("Project_Volume__c")).toString();
            String PROJECT__C = handleString(s.getField("Project__c")).toString();
            String PURCHASE_ORDER_String__C =
                handleString(s.getField("Purchase_Order_Date__c")).toString();
            String QUANTITY_PER_MONTH__C =
                handleString(s.getField("Quantity_per_Month__c")).toString();
            String QUOTE_BILL_TO_PAYMENT_TERMS__C =
                handleString(s.getField("Quote_Bill_To_Payment_Terms__c")).toString();
            String QUOTE_BILL_TO__C = handleString(s.getField("Quote_Bill_To__c")).toString();
            String QUOTE_CONDITIONS__C = handleString(s.getField("Quote_Conditions__c")).toString();
            String QUOTE_SENT_TO_CUSTOMER__C =
                handleString(s.getField("Quote_Sent_to_Customer__c")).toString();
            String QUOTE_STAGE_VALUE_ROLLUP__C =
                handleString(s.getField("Quote_Stage_Value_Rollup__c")).toString();
            String RECORDTYPEID = handleString(s.getField("RecordTypeId")).toString();
            String REP__C = handleString(s.getField("REP__c")).toString();
            String REQUEST_FOR_BUDGETARY_QUOTE__C =
                handleString(s.getField("Request_for_Budgetary_Quote__c")).toString();
            String REQUEST_FOR_RE_QUOTE__C =
                handleString(s.getField("Request_for_Re_quote__c")).toString();
            String RE_QUOTE__C = handleString(s.getField("Re_Quote__c")).toString();
            String RFQ__C = handleString(s.getField("RFQ__c")).toString();
            String ROLLUPCOMPETITORS__C =
                handleString(s.getField("RollUpCompetitors__c")).toString();
            String ROLLUP_ACTUAL_DESIGN_WIN_String_ =
                handleString(s.getField("Rollup_Actual_Design_Win_Date__c")).toString();
            String RUNNING_USER__C = handleString(s.getField("Running_User__c")).toString();
            String SALES_REGION__C = handleString(s.getField("Sales_Region__c")).toString();
            String SEGMENTATION__C = handleString(s.getField("Segmentation__c")).toString();
            String SFID__C = handleString(s.getField("SFID__c")).toString();
            String SHIPPING_TERMS__C = handleString(s.getField("Shipping_Terms__c")).toString();
            String SINGLE_MULTI_SOURCE__C =
                handleString(s.getField("Single_Multi_Source__c")).toString();
            String SOW_SENT_TO_BU__C = handleString(s.getField("SOW_Sent_to_BU__c")).toString();
            String STAGENAME = handleString(s.getField("StageName")).toString();
            String STANDARDIZED_OPPORTUNITY_NAME_ =
                handleString(s.getField("Standardized_Opportunity_Name_Indexed__c")).toString();
            String STRATEGIC__C = handleString(s.getField("Strategic__c")).toString();
            String SUBMIT_FOR_QUOTE__C = handleString(s.getField("Submit_for_Quote__c")).toString();
            String SYNAPTICS_SHARE_PERCENTAGE__C =
                handleString(s.getField("Synaptics_Share_Percentage__c")).toString();
            String SYNA_PART_NUMBER__C = handleString(s.getField("SYNA_Part_Number__c")).toString();
            String SYNA_SHARE__C = handleString(s.getField("SYNA_share__c")).toString();
            String SYNCEDQUOTEID = handleString(s.getField("SyncedQuoteId")).toString();
            String SYSTEMMODSTAMP = handleString(s.getField("SystemModstamp")).toString();
            String TOTAL_CUSTOMER_PROJECT_QTY__C =
                handleString(s.getField("Total_Customer_Project_Qty__c")).toString();
            String TOTAL_OPPORTUNITY_FAE_INVOLVEM =
                handleString(s.getField("Total_Opportunity_FAE_Involvement__c")).toString();
            String TYPE = handleString(s.getField("Type")).toString();
            String WHITEBOX__C = handleString(s.getField("Whitebox__c")).toString();
            String WIN_RATE__C = handleString(s.getField("Win_Rate__c")).toString();
            String FORECAST_QU_PRI_PRO_NAME_C =
                handleString(s.getField("Forecast_Quote_Primary_Product_Name__c")).toString();
            String STAND_OPPO_NAME_INDEXED_C =
                handleString(s.getField("Standardized_Opportunity_Name_Indexed__c")).toString();
            preparedStatement1.setString(1, ACCOUNTID);
            preparedStatement1.setString(2, ACTUALS_AMOUNT__C);
            preparedStatement1.setString(3, ACTUAL_DESIGN_IN_DATE__C);
            preparedStatement1.setString(4, ACTUAL_SYNA_MP_String__C);
            preparedStatement1.setString(5, AMOUNT);
            preparedStatement1.setString(6, APPLICATION__C);
            preparedStatement1.setString(7, APTS_ALLOW_OPPORTUNITY_STAGE_C);
            preparedStatement1.setString(8, APTS_PROJECT_TYPE__C);
            preparedStatement1.setString(9, APTTUS_APPROVAL__APPROVAL_STAT);
            preparedStatement1.setString(10, APTTUS_PROPOSAL__DISCOUNT_PERC);
            preparedStatement1.setString(11, ASP__C);
            preparedStatement1.setString(12, AUDIT_ACTUAL_DW_VS_CLOSEString__);
            preparedStatement1.setString(13, AVERAGE_MONTHLY_QUANTITY__C);
            preparedStatement1.setString(14, BU_PRICE_APPROVED_String__C);
            preparedStatement1.setString(15, BUDGETARY__C);
            preparedStatement1.setString(16, BUDGETARY_PRICE_APPROVAL_String_);
            preparedStatement1.setString(17, CAMPAIGNID);
            preparedStatement1.setString(18, CHANNEL__C);
            preparedStatement1.setString(19, CLEARPAD_SERIES__C);
            preparedStatement1.setString(20, CLOSEString);
            preparedStatement1.setString(21, CLOSED_String_PAST_DUE__C);
            preparedStatement1.setString(22, COUNT_OF_BU_REQUESTED_TIMESTAM);
            preparedStatement1.setString(23, CREATEDBYID);
            preparedStatement1.setString(24, CREATEDString);
            preparedStatement1.setString(25, CURRENT_SALES_NET_PRICE__C);
            preparedStatement1.setString(26, CUSTOMER_ID__C);
            preparedStatement1.setString(27, CUSTOMER_PART_NUMBER__C);
            preparedStatement1.setString(28, "null");
            preparedStatement1.setString(29, CUSTOMER_SPECIFIC_QUOTE_CONDIT);
            preparedStatement1.setString(30, DAYS_SINCE_LAST_MODIFIED_BY_US);
            preparedStatement1.setString(31, DAYS_TO_CLOSE_String__C);
            preparedStatement1.setString(32, DAYS_TO_DESIGN_IN__C);
            preparedStatement1.setString(33, DESCRIPTION);
            preparedStatement1.setString(34, DESIGN_IN_String__C);
            preparedStatement1.setString(35, DISPLAY_SIZE_TYPE__C);
            preparedStatement1.setString(36, DIVISION__C);
            preparedStatement1.setString(37, EARLY_SAMPLE_String__C);
            preparedStatement1.setString(38, ENGAGEMENT_MODEL__C);
            preparedStatement1.setString(39, ESTIMATED_REVENUE__C);
            preparedStatement1.setString(40, EXPECTED_DESIGN_WIN_String__C);
            preparedStatement1.setString(41, EXPECTED_RFI_RFQ_String__C);
            preparedStatement1.setString(42, FIRST_CUSTOMER_PO_NUMBER__C);
            preparedStatement1.setString(43, FORECASTCATEGORY);
            preparedStatement1.setString(44, FORECASTCATEGORYNAME);
            preparedStatement1.setString(45, FORECAST_QUOTE_PRIMARY_PRODUCT);
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
            preparedStatement1.setString(61, LASTACTIVITYString);
            preparedStatement1.setString(62, LASTMODIFIEDBYID);
            preparedStatement1.setString(63, LASTMODIFIEDString);
            preparedStatement1.setString(64, LASTREFERENCEDString);
            preparedStatement1.setString(65, LASTVIEWEDString);
            preparedStatement1.setString(66, LAST_UPStringD_BY_USER__C);
            preparedStatement1.setString(67, LCM_OPPORTUNITY_LOOKUP__C);
            preparedStatement1.setString(68, LCM_REFERENCE_ACCOUNT__C);
            preparedStatement1.setString(69, LCM_TO_OEM_REFERENCE_ACCOUNT__);
            preparedStatement1.setString(70, LCM_TO_OEM_REFERENCE_OPPORTUNI);
            preparedStatement1.setString(71, LEADSOURCE);
            preparedStatement1.setString(72, LIFETIME_QUANTITY_TIER__C);
            preparedStatement1.setString(73, LIFETIME_QUANTITY__C);
            preparedStatement1.setString(74, MARKET_SEGMENT_CODE__C);
            preparedStatement1.setString(75, MOBILE_OS__C);
            preparedStatement1.setString(76, MODEL_NAME_IN_MARKET__C);
            preparedStatement1.setString(77, MOQ__C);
            preparedStatement1.setString(78, MP_String__C);
            preparedStatement1.setString(79, MP_FORECAST_FLAG__C);
            preparedStatement1.setString(80, NAME);
            preparedStatement1.setString(81, NEXTSTEP);
            preparedStatement1.setString(82, NOTEBOOK_LCD_SIZE__C);
            preparedStatement1.setString(83, NUMBER_OF_COMPETITORS__C);
            preparedStatement1.setString(84, NUMBER_OF_DAYS_PAST_DUE__C);
            preparedStatement1.setString(85, NUMBER_OF_MONTHS_LIFETIME__C);
            preparedStatement1.setString(86, NUMBER_OF_OPPORTUNITIES__C);
            preparedStatement1.setString(87, NUMBER_OF_PRODUCTS__C);
            preparedStatement1.setString(88, NUMBER_OF_QUOTES_WITH_PRODUCTS);
            preparedStatement1.setString(89, NUMBER_OF_QUOTES__C);
            preparedStatement1.setString(90, ODM_PROGRAM_NAME__C);
            preparedStatement1.setString(91, ODM_REFERENCE_ACCOUNT__C);
            preparedStatement1.setString(92, ODM_SUGGESTED_FSF__C);
            preparedStatement1.setString(93, OEM_OPPORTUNITY_LOOKUP__C);
            preparedStatement1.setString(94, OEM_OPPORTUNITY_NAME__C);
            preparedStatement1.setString(95, OEM_PROGRAM_NAME_INPUT__C);
            preparedStatement1.setString(96, OEM_REFERENCE_ACCOUNT_ID__C);
            preparedStatement1.setString(97, OEM_REFERENCE_ACCOUNT__C);
            preparedStatement1.setString(98, OEM_SUGGESTED_FSF__C);
            preparedStatement1.setString(99, OEM_VAR_ACCOUNT__C);
            preparedStatement1.setString(100, OF_ADDITIONAL_QUOTES__C);
            preparedStatement1.setString(101, OF_RELATED_ODM_S__C);
            preparedStatement1.setString(102, OID__C);
            preparedStatement1.setString(103, OMIT_FROM_FORECAST__C);
            preparedStatement1.setString(104, OPA_SAME_ACCOUNT_ROLLUP__C);
            preparedStatement1.setString(105, OPPORTUNITY_FORECAST__C);
            preparedStatement1.setString(106, OPPORTUNITY_LIFETIME_QUANTITY_);
            preparedStatement1.setString(107, OPPORTUNITY_OWNER_EMP_ID__C);
            preparedStatement1.setString(108, OPPORTUNITY_REPORT_NAME__C);
            preparedStatement1.setString(109, OPPORTUNITY_STAGE_SCORE__C);
            preparedStatement1.setString(110, OPPORTUNITY_VOLUME__C);
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
            preparedStatement1.setString(126, PRIMARY_APPROVED__C);
            preparedStatement1.setString(127, PRIMARY_CONTACT__C);
            preparedStatement1.setString(128, PRIMARY_PRODUCT_STATUS__C);
            preparedStatement1.setString(129, PRIMARY_QUOTE_BILL_TO_NAME__C);
            preparedStatement1.setString(130, PRIMARY_SOLUTION__C);
            preparedStatement1.setString(131, PROBABILITY);
            preparedStatement1.setString(132, PRODUCT_GROUP__C);
            preparedStatement1.setString(133, PRODUCT_TYPE2__C);
            preparedStatement1.setString(134, PRODUCT__C);
            preparedStatement1.setString(135, PROJECT_ACCOUNT_NAME_S_REGION_);
            preparedStatement1.setString(136, PROJECT_MODEL_NAME_IN_MARKET__);
            preparedStatement1.setString(137, PROJECT_VOLUME__C);
            preparedStatement1.setString(138, PROJECT__C);
            preparedStatement1.setString(139, PURCHASE_ORDER_String__C);
            preparedStatement1.setString(140, QUANTITY_PER_MONTH__C);
            preparedStatement1.setString(141, QUOTE_BILL_TO_PAYMENT_TERMS__C);
            preparedStatement1.setString(142, QUOTE_BILL_TO__C);
            preparedStatement1.setString(143, QUOTE_CONDITIONS__C);
            preparedStatement1.setString(144, QUOTE_SENT_TO_CUSTOMER__C);
            preparedStatement1.setString(145, QUOTE_STAGE_VALUE_ROLLUP__C);
            preparedStatement1.setString(146, RECORDTYPEID);
            preparedStatement1.setString(147, REP__C);
            preparedStatement1.setString(148, REQUEST_FOR_BUDGETARY_QUOTE__C);
            preparedStatement1.setString(149, REQUEST_FOR_RE_QUOTE__C);
            preparedStatement1.setString(150, RE_QUOTE__C);
            preparedStatement1.setString(151, RFQ__C);
            preparedStatement1.setString(152, ROLLUPCOMPETITORS__C);
            preparedStatement1.setString(153, ROLLUP_ACTUAL_DESIGN_WIN_String_);
            preparedStatement1.setString(154, RUNNING_USER__C);
            preparedStatement1.setString(155, SALES_REGION__C);
            preparedStatement1.setString(156, SEGMENTATION__C);
            preparedStatement1.setString(157, SFID__C);
            preparedStatement1.setString(158, SHIPPING_TERMS__C);
            preparedStatement1.setString(159, SINGLE_MULTI_SOURCE__C);
            preparedStatement1.setString(160, SOW_SENT_TO_BU__C);
            preparedStatement1.setString(161, STAGENAME);
            preparedStatement1.setString(162, STANDARDIZED_OPPORTUNITY_NAME_);
            preparedStatement1.setString(163, STRATEGIC__C);
            preparedStatement1.setString(164, SUBMIT_FOR_QUOTE__C);
            preparedStatement1.setString(165, SYNAPTICS_SHARE_PERCENTAGE__C);
            preparedStatement1.setString(166, SYNA_PART_NUMBER__C);
            preparedStatement1.setString(167, SYNA_SHARE__C);
            preparedStatement1.setString(168, SYNCEDQUOTEID);
            preparedStatement1.setString(169, SYSTEMMODSTAMP);
            preparedStatement1.setString(170, TOTAL_CUSTOMER_PROJECT_QTY__C);
            preparedStatement1.setString(171, TOTAL_OPPORTUNITY_FAE_INVOLVEM);
            preparedStatement1.setString(172, TYPE);
            preparedStatement1.setString(173, WHITEBOX__C);
            preparedStatement1.setString(174, WIN_RATE__C);
            preparedStatement1.setString(175, FORECAST_QU_PRI_PRO_NAME_C);
            preparedStatement1.setString(176, STAND_OPPO_NAME_INDEXED_C);
            preparedStatement1.addBatch();
            count1++;
            if (count1 % batchSize == 0) {
              System.out.println("Commit the batch");
              preparedStatement1.executeBatch();
              connection1.commit();
            }
            System.out.println("Inserting into Opportunity Id: " + ID);
          }
          if (qr.isDone()) {
            done = true;
          } else {
            qr = sourceConnection.queryMore(qr.getQueryLocator());
          }
        }
        System.out.println("Final Commit the batch");
        preparedStatement1.executeBatch();
        connection1.commit();
      }
    } catch (Exception e) {
      e.printStackTrace(o);
      result = "Failure";
    } finally {
      if (preparedStatement2 != null) {
        try {
          preparedStatement2.close();
        } catch (SQLException e) {
          e.printStackTrace(o);
        }
      }
      if (resultSet1 != null) {
        try {
          resultSet1.close();
        } catch (SQLException e) {
          e.printStackTrace(o);
        }
      }
      if (connection2 != null) {
        try {
          connection2.close();
        } catch (SQLException e) {
          e.printStackTrace(o);
        }
      }
      if (preparedStatement1 != null) {
        try {
          preparedStatement1.close();
        } catch (SQLException e) {
          e.printStackTrace(o);
        }
      }
      if (connection1 != null) {
        try {
          connection1.close();
        } catch (SQLException e) {
          e.printStackTrace(o);
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
      emailId = properties.getProperty("emailId");
      DB_Name = properties.getProperty("DB_Name");
      urlSourcePartner = properties.getProperty("urlSourcePartner");
      urlDestination = properties.getProperty("urlDestination");
      usernameSource = properties.getProperty("usernameSource");
      passwordSource = properties.getProperty("passwordSource");
      usernameDestination = properties.getProperty("usernameDestination");
      passwordDestination = properties.getProperty("passwordDestination");
      lastRunnedTime = properties.getProperty("lastRunnedTime");
      System.out.println("Last Runned Time = " + lastRunnedTime);
    } catch (FileNotFoundException e) {
      e.printStackTrace(o);
      result = "Failure";
    } catch (IOException e) {
      e.printStackTrace(o);
      result = "Failure";
    }
  }

  /**
   * To get connection from destination.
   */
  private static Connection getConnection() {
    final String URL = urlDestination;
    final String USERNAME = usernameDestination;
    final String PASSWORD = passwordDestination;
    Connection connection = null;
    try {
      Class.forName("oracle.jdbc.driver.OracleDriver");
      connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
    } catch (Exception e) {
      e.printStackTrace(o);
      result = "Failure";
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

  private static String handleString(Object str) {
    if (str == null) {
      String a = "null";
      return a;
    } else {
      String s = str.toString();
      s = s.toString().substring(0, Math.min(s.length(), 300));
      return s;
    }
  }
}
