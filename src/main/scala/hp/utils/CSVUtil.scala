package hp.utils

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.nio.charset.Charset
import java.util.ArrayList

import com.csvreader.CsvReader

/**
 * @author lixinga
 */
object CSVUtil {
  
  def main(args: Array[String]): Unit = { 
   val csv="C:/Users/lixinga.AUTH/Desktop/prepare/ggggg.csv"
   val csvencode="GB2312"
   val StopWord="C:/work/manufacturer/workspace/callct/src/main/resources/StopWordsU.txt"
   val StopWordencode="UTF8"
   val NosplitWord="C:/work/manufacturer/workspace/callct/src/main/resources/NoSplitWords.txt"
   val Nosplitencode="UTF8"
   
   var al =readCSV(csv,csvencode)
   
   
//   val st=readfile(StopWord,StopWordencode)
//   val ns=readfile(NosplitWord,Nosplitencode)
   
//  var it= st.iterator()
//  while(it.hasNext()){
//    println(it.next())
//  }
  }
  def readCSV(filePath: String, code: String)={
     //生成CsvReader对象，以，为分隔符，GBK编码方式
    val r = new CsvReader(filePath, ',', Charset.forName(code));
    //读取表头
//    r.readHeaders();
    //逐条读取记录，直至读完
    var count = 0
    var al = new ArrayList[CsvDom]();
    while (r.readRecord()) {

      al.add(CsvDom(r.get(0), r.get(1)))
      //读取一条记录

      count = count + r.get(0).length() + r.get(1).length()
      if (count < 100)
        println(r.get(0) + "-------------------" + r.get(1))
    }
    r.close();
    println(count)
    al
  }
  def readfile(filePath: String, code: String) = {
    var bufferedReader: BufferedReader = null;
    var file: File = new File(filePath);
    var al = new ArrayList[String]();
    if (file.isFile() && file.exists()) { // 判断文件是否存在 
      var read = new InputStreamReader(new FileInputStream(
        file), code); // 考虑到编码格式 
      bufferedReader = new BufferedReader(read);
      var line = bufferedReader.readLine(); // 读取第一行 

      while (line != null) { // 如果 line 为空说明读完了 
        al.add(line); // 将读到的内容添加到 buffer 中 
        // buffer.append("\n"); // 添加换行符 
        line = bufferedReader.readLine(); // 读取下一行 
      }
      bufferedReader.close()
      read.close()
    }
    al
  }
case class CsvDom(classa: String, text: String) 
}