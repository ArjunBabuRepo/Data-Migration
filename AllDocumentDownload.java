package documentdownload;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import com.agile.api.AgileSessionFactory;
import com.agile.api.IAgileSession;
import com.agile.api.IAttachmentFile;
import com.agile.api.IItem;
import com.agile.api.IQuery;
import com.agile.api.IRow;
import com.agile.api.ITable;
import com.agile.api.ItemConstants;

/*
 * This program downloads documents with attachments and starts with 5. It also dynamically
 * organizes the downloaded files into its respective Title Block Number and all Title Block Numbers
 * into its respective Prefix Numbers.
 */
public class AllDocumentDownload {
  private static final String USERNAME = "zzz";
  private static final String PASSWORD = "zzz";
  private static final String URL = "zzz";
  private static IAgileSession session = null;
  private static AgileSessionFactory factory;

  public static void main(String[] args) {
    try {
      factory = AgileSessionFactory.getInstance(URL);
      HashMap params = new HashMap();
      params.put(AgileSessionFactory.USERNAME, USERNAME);
      params.put(AgileSessionFactory.PASSWORD, PASSWORD);
      session = factory.createSession(params);
      System.out.println("Session created");
      IQuery query =
          (IQuery) session.createObject(IQuery.OBJECT_TYPE, ItemConstants.CLASS_DOCUMENTS_CLASS);
      query.setCaseSensitive(false);
      query.setCriteria("[Title Block.Number] starts with '5' and ["
          + ItemConstants.ATT_ATTACHMENTS_FILE_NAME + "] is not null");
      ITable table = query.execute();
      Iterator<?> itQ = table.iterator();
      System.out.println("Query Set");
      StringBuilder titleBlockNumber = new StringBuilder();
      StringBuilder fileName = new StringBuilder();
      int fileCount = 0;
      int rowNum = 0;
      int limit = 0;
      int limitfileCount = 3292;
      // Iterate through all the documents with attachments and starts with 5.
      while (itQ.hasNext()) {
        // To resume the download.
        while (limit != (limitfileCount - 1)) {
          IRow row1 = (IRow) itQ.next();
          limit++;
          ++rowNum;
        }
        IRow row1 = (IRow) itQ.next();
        titleBlockNumber.append(row1.getValue(ItemConstants.ATT_TITLE_BLOCK_NUMBER));
        System.out.println("Query document row = " + titleBlockNumber);
        IItem doc = (IItem) session.getObject(ItemConstants.CLASS_DOCUMENTS_CLASS,
            row1.getValue(ItemConstants.ATT_TITLE_BLOCK_NUMBER));
        System.out.println("Query row number = " + (++rowNum));
        // Get the attachment table for file attachments
        // Get a table iterator
        Iterator itA = doc.getAttachments().iterator();
        int flag = 0;
        // Downloads all the attachments of the current document.
        while (itA.hasNext()) {
          IRow row2 = (IRow) itA.next();
          fileName.append(row2.getName());
          String last3Letters = fileName.substring((fileName.length()) - 3);
          String prefixNumber = titleBlockNumber.substring(0, 3);
          if (last3Letters.equalsIgnoreCase("pdf") || last3Letters.equalsIgnoreCase("xls")
              || last3Letters.equalsIgnoreCase("lsx") || last3Letters.equalsIgnoreCase("doc")
              || last3Letters.equalsIgnoreCase("ocx") || last3Letters.equalsIgnoreCase("ppt")
              || last3Letters.equalsIgnoreCase("ptx") || last3Letters.equalsIgnoreCase("txt")
              || last3Letters.equalsIgnoreCase("zip")) {
            if (flag == 0) {
              new File("D:/OneDrive - Snuvik Technologies Pvt Ltd/Document Project Synaptics/"
                  + prefixNumber + "/" + titleBlockNumber).mkdir();
              System.out.println("Document Created = " + titleBlockNumber);
              ++flag;
            }
            System.out.println("fileCount = " + (++fileCount));
            System.out.println(fileName);
            // Read the contents of the stream
            InputStream stream = ((IAttachmentFile) row2).getFile();
            FileOutputStream outputStream = new FileOutputStream(
                "D:/OneDrive - Snuvik Technologies Pvt Ltd/Document Project Synaptics/"
                    + prefixNumber + "/" + titleBlockNumber + "/" + fileName);
            int read;
            byte[] bytes = new byte[1024];
            while ((read = stream.read(bytes)) != -1) {
              outputStream.write(bytes, 0, read);
            }
            outputStream.close();
          }
          fileName.delete(0, fileName.length());
        }
        titleBlockNumber.delete(0, titleBlockNumber.length());
      }
      session.close();
      System.out.println("Done");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
