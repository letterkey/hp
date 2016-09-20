package hp.utils

import java.util.HashMap

import classficate.callct.LabeledRecord
import classficate.callct.utils.CSVUtil
import org.ansj.domain.Term
import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.NlpAnalysis
import org.ansj.util.FilterModifWord
import org.nlpcn.commons.lang.tire.domain.Value
import org.nlpcn.commons.lang.tire.library.Library

object MySplitwdUtil{
  var csv = "C:/Users/lixinga.AUTH/Desktop/prepare/ggggg.csv"
  var csvencode = "GB2312"
  var StopWord = "C:/work/manufacturer/workspace/callct/src/main/resources/StopWordsU.txt"
  var StopWordencode = "UTF8"
  var NosplitWord = "C:/work/manufacturer/workspace/callct/src/main/resources/NoSplitWords.txt"
  var Nosplitencode = "UTF8"
  var mod=300
  def main(args: Array[String]): Unit = {
    genlvsm
    //    var str = "欢迎使用ansj_seg,(ansj中文分词)在这里如果你遇到什么问题都可以联系我.并且但是我一定尽我所能.帮助大家.ansj_seg更快,更准,更自由!" ;
    //println(ToAnalysis.parse(str));
  }
  var train: List[LabeledRecord] = Nil
  var test: List[LabeledRecord] = Nil

  def init = {
    val st = CSVUtil.readfile(StopWord, StopWordencode)
    val ns = CSVUtil.readfile(NosplitWord, Nosplitencode)
    var it = st.iterator()
    while (it.hasNext()) {
      FilterModifWord.insertStopWord(it.next())
    }
    it = ns.iterator()
    while (it.hasNext()) {
      var aa = it.next()
      val value = new Value(aa, aa, "n");
      Library.insertWord(UserDefineLibrary.ambiguityForest, value);
    }
    FilterModifWord.insertStopNatures("w");
    FilterModifWord.insertStopNatures("nr");
  }
  init

  def processOne(text: String) = {
    var parseResultList = FilterModifWord.modifResult(NlpAnalysis.parse(text))
    var itpr = parseResultList.iterator()
    var sb = new StringBuilder();
    while (itpr.hasNext()) {
      var abc = itpr.next()
      sb.append(abc.getName + " ")
    }
    sb.toString().trim()
  }
  def process = {

    val st = CSVUtil.readfile(StopWord, StopWordencode)
    val ns = CSVUtil.readfile(NosplitWord, Nosplitencode)
    var al = CSVUtil.readCSV(csv, csvencode)
    // 添加停止词
    var it = st.iterator()
    while (it.hasNext()) {
      FilterModifWord.insertStopWord(it.next())
    }
    it = ns.iterator()
    while (it.hasNext()) {
      var aa = it.next()
      val value = new Value(aa, aa, "n")
      Library.insertWord(UserDefineLibrary.ambiguityForest, value)
    }

    FilterModifWord.insertStopNatures("w")
    FilterModifWord.insertStopNatures("nr")

    var itd = al.iterator()
    val parseResultList: List[Term] = null
    var counter = 0
    var hm = new HashMap[String, Integer]()
    var itd2 = al.iterator()
    while (itd2.hasNext()) {
      var dm = itd2.next()
      hm.put(dm.classa, if (hm.containsKey(dm.classa)) hm.get(dm.classa) + 1 else 1)
    }
    var hm1 = new HashMap[String, Integer]()
    var itt = hm.entrySet().iterator()
    while (itt.hasNext()) {

      var abb = itt.next()
      if (abb.getValue == 1) {
        hm1.put(abb.getKey, 1)
      }
    }
    while (itd.hasNext()) {
      counter = counter + 1
      var dm = itd.next()
      if (!hm1.containsKey(dm.classa)) {
        var parseResultList = FilterModifWord.modifResult(NlpAnalysis.parse(dm.text))
        var itpr = parseResultList.iterator()
        var sb = new StringBuilder();
        while (itpr.hasNext()) {
          var abc = itpr.next()
          sb.append(abc.getName + " ")
          //       println(abc.getName+"++++++++"+abc.getRealName)
          //       hm.put(abc.getName, 1)
        }
        if (counter % mod == 0) {
          test = LabeledRecord(dm.classa, sb.toString().trim(),dm.text) :: test
        } else {
          //      hm.put(dm.classa, 1)
          train = LabeledRecord(dm.classa, sb.toString().trim(),dm.text) :: train
        }
        //      println(parseResultList.toArray());
      }
    }
  }
  def genlvsm = {

    val st = CSVUtil.readfile(StopWord, StopWordencode)
    val ns = CSVUtil.readfile(NosplitWord, Nosplitencode)
    var al = CSVUtil.readCSV(csv, csvencode)

    var it = st.iterator()
    while (it.hasNext()) {
      FilterModifWord.insertStopWord(it.next())
    }
    it = ns.iterator()
    while (it.hasNext()) {
      var aa = it.next()
      val value = new Value(aa, aa, "n");
      Library.insertWord(UserDefineLibrary.ambiguityForest, value);
    }

    FilterModifWord.insertStopNatures("w");
    FilterModifWord.insertStopNatures("nr");
    var itd = al.iterator()
    val parseResultList: List[Term] = null
    var counter = 0
    var hm = new HashMap[String, Integer]()
    var hmwd = new HashMap[String, Integer]()
    var wordIndex = new HashMap[String, Integer]()
    var classIndex = new HashMap[String, Integer]()
    var indexword = 0
    var indexclass = 0
    while (itd.hasNext()) {
      counter = counter + 1
      var dm = itd.next()
      var parseResultList = FilterModifWord.modifResult(NlpAnalysis.parse(dm.text))
      var itpr = parseResultList.iterator()
      var sb = new StringBuilder();
      var oneitem = new HashMap[String, Integer]()
      while (itpr.hasNext()) {
        var abc = itpr.next()
        sb.append(abc.getName + " ")
        if (!wordIndex.containsKey(abc.getName)) {
          wordIndex.put(abc.getName, indexword)
          indexword = indexword + 1
        }
        //       println(abc.getName+"++++++++"+abc.getRealName)
               hmwd.put(abc.getName, 1)
      }

      if (!classIndex.containsKey(sb.toString().trim())) {
        classIndex.put(sb.toString().trim(), indexclass)
        indexclass = indexclass + 1
      }
      hm.put(dm.classa, if (hm.containsKey(dm.classa)) hm.get(dm.classa) + 1 else 1)
      train = LabeledRecord(dm.classa, sb.toString().trim(),dm.text) :: train
      //        println(sb.toString().trim())
      //      println(parseResultList.toArray());
    }
    //       println( hm.keySet().size())
    var abcount = 0
    var itt = hm.entrySet().iterator()
    while (itt.hasNext()) {

      var abb = itt.next()
      if (abb.getValue > 1) {
        abcount = abcount + 1;
        println(abb.getKey + "---" + abb.getValue)
      }
    }
    println(abcount)
    println(hmwd.keySet().size())
  }

}

