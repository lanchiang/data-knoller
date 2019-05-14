package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.config.DataLoadingConfig;

public class DetectLanguageTest extends DataLoadingConfig {

//    @BeforeClass
//    public static void setUp() {
////        transforms = DataLoadingConfig.createTransformsManually();
//        DataLoadingConfig.basicSetup();
//    }
//
//    @Test
//    public void testLanguageDetection() throws Exception {
//        AbstractPreparator abstractPreparator = new DetectLanguagePreparator("stemlemma", 5);
//
//        AbstractPreparation preparation = new Preparation(abstractPreparator);
//        pipeline.addPreparation(preparation);
//        pipeline.executePipeline();
//
//        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);
//        List<ErrorLog> errorLogs = new ArrayList<>();
//        ErrorRepository errorRepository = new ErrorRepository(errorLogs);
//        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
//
//        Map<Integer, LanguageMetadataOld.LanguageEnum> langMapping = new HashMap<>();
//        langMapping.put(0, LanguageMetadataOld.LanguageEnum.SPANISH);
//        langMapping.put(1, LanguageMetadataOld.LanguageEnum.ENGLISH);
//        Assert.assertTrue(pipeline.getMetadataRepository().containByValue(new LanguageMetadataOld("stemlemma", langMapping)));
//    }
//
//    @Test
//    public void testUnsupportedLanguages() throws Exception {
//        AbstractPreparator abstractPreparator = new DetectLanguagePreparator("stemlemma", 2);
//
//        AbstractPreparation preparation = new Preparation(abstractPreparator);
//        pipeline.addPreparation(preparation);
//        pipeline.executePipeline();
//
//        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);
//
//        List<ErrorLog> realErrorLogs = pipeline.getErrorRepository().getErrorLogs();
//        Assert.assertEquals(2, realErrorLogs.size());
//
//        PreparationErrorLog italianError = (PreparationErrorLog) realErrorLogs.get(0);
//        Assert.assertEquals("DetectLanguagePreparator", italianError.getPreparation().getName());
//        Assert.assertEquals("LanguageMetadataOld class org.languagetool.language.Italian not supported",
//                            italianError.getErrorMessage());
//        Assert.assertEquals("stemlemma", italianError.getValue());
//
//        PreparationErrorLog bretonError = (PreparationErrorLog) realErrorLogs.get(1);
//        Assert.assertEquals("DetectLanguagePreparator", bretonError.getPreparation().getName());
//        Assert.assertEquals("LanguageMetadataOld class org.languagetool.language.Breton not supported",
//                bretonError.getErrorMessage());
//        Assert.assertEquals("stemlemma", bretonError.getValue());
//    }
}
