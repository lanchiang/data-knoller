package de.hpi.isg.dataprep

import org.scalatest.{FlatSpecLike, Matchers}

class DateScorerTest extends FlatSpecLike with Matchers {
  val testVocabulary = "test_vocab.csv"

  "Dates" should "be scored" in {
    val scorer = new DateScorer()
    val threshold = 0.5f
    scorer.score("hahaha") should be < threshold
    scorer.score("12-31-2013") should be > threshold
    scorer.score("21-11--1312") should be > threshold
  }

  "Date scorer" should "load a different vocabulary" in {
    val scorer = new DateScorer()
    scorer.vocabulary should not be empty
    val oldSize = scorer.vocabulary.size
    scorer.loadVocabulary(getClass.getResource(s"/date_format/$testVocabulary"))
    scorer.vocabulary should not be empty
    scorer.vocabulary.size should not equal oldSize
  }

  it should "use the correct oov index" in {
    val scorer = new DateScorer()
    scorer.oovTokenIndex() shouldEqual 0
    scorer.loadVocabulary(getClass.getResource(s"/date_format/$testVocabulary"))
    scorer.oovTokenIndex() shouldEqual 1
  }

  it should "tokenize strings" in {
    val scorer = new DateScorer()
    scorer.loadVocabulary(getClass.getResource(s"/date_format/$testVocabulary"))
    val input = "$%&%%&&"
    val tokens = scorer.tokenize(input)
    tokens shouldEqual List(5, 6, 7, 6, 6, 7, 7)
  }

  it should "tokenize strings containing unknown tokens" in {
    val scorer = new DateScorer()
    // this is a korean symbol
    val unknownChar = 1231323.toChar.toString
    val input = "$abc" + unknownChar
    var tokens = scorer.tokenize(input)
    tokens shouldEqual List(5, 66, 67, 68, 0)

    scorer.loadVocabulary(getClass.getResource(s"/date_format/$testVocabulary"))
    tokens = scorer.tokenize(input)
    // now the oov token is 1
    tokens shouldEqual List(5, 1, 1, 1, 1)
  }

  it should "score single char strings without error" in {
    val scorer = new DateScorer()
    scorer.score("1") shouldEqual 0.0f
    scorer.score("a") shouldEqual 0.0f
    scorer.score("") shouldEqual 0.0f
  }
}
