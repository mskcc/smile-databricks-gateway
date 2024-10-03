package smile_databricks_gateway

import (
	"encoding/json"
)

var RequestJSON = `
{
  "smileRequestId": "6cca6166-875a-11eb-ae9e-acde48001122",
  "igoRequestId": "IGO_TEST_REQUEST",
  "genePanel": "GENESET101_BAITS",
  "projectManagerName": "marge simpson",
  "piEmail": "",
  "labHeadName": "bart simpson",
  "labHeadEmail": "bart@mskcc.org",
  "investigatorName": "lisa simpson",
  "investigatorEmail": "lisa@mskcc.org",
  "dataAnalystName": "",
  "dataAnalystEmail": "",
  "otherContactEmails": "simpsons@mskcc.org",
  "dataAccessEmails": "",
  "qcAccessEmails": "",
  "isCmoRequest": true,
  "bicAnalysis": false,
  "samples": [
    {
      "smileSampleId": "afe74fba-8756-11eb-9b45-acde48001122",
      "smilePatientId": "6cc7394f-875a-11eb-91ec-acde48001122",
      "cmoSampleName": "brooklyn sluggers",
      "sampleName": "IGO_TEST_SAMPLE",
      "sampleType": "Normal",
      "oncotreeCode": "TPLL",
      "collectionYear": "",
      "tubeId": "4157451784",
      "cfDNA2dBarcode": "8029250670",
      "qcReports": [
        {
          "qcReportType": "LIBRARY",
          "comments": "",
          "investigatorDecision": "Continue processing"
        }
      ],
      "libraries": [
        {
          "libraryIgoId": "22022_CC_3_1",
          "libraryConcentrationNgul": 34.2,
          "captureConcentrationNm": "1.461988304093567",
          "captureInputNg": "50.0",
          "captureName": "Pool-22022_BZ-22022_CC-Tube7_1",
          "runs": [
            {
              "runMode": "HiSeq High Output",
              "runId": "CRX_7395",
              "flowCellId": "HGJMLBBXY",
              "readLength": "101/8/8/101",
              "runDate": "2020-05-20",
              "flowCellLanes": [
                1,
                2,
                3,
                4,
                5,
                6,
                7
              ],
              "fastqs": [
                "/FASTQ/Project_22022_CC/Sample_LMNO_4396_N_IGO_22022_CC_3/LMNO_4396_N_IGO_22022_CC_3_S144_R1_001.fastq.gz",
                "/FASTQ/Project_22022_CC/Sample_LMNO_4396_N_IGO_22022_CC_3/LMNO_4396_N_IGO_22022_CC_3_S144_R2_001.fastq.gz"
              ]
            }
          ]
        }
      ],
      "cmoPatientId": "C-TX6DNG",
      "primaryId": "22022_CC_3",
      "investigatorSampleId": "LMNO_4396_N",
      "species": "Human",
      "sex": "F",
      "tumorOrNormal": "Normal",
      "preservation": "EDTA-Streck",
      "sampleClass": "Blood",
      "sampleOrigin": "Buffy Coat",
      "tissueLocation": "Blood",
      "baitSet": "GENESET101_BAITS",
      "genePanel": "GENESET101_BAITS",
      "datasource": "igo",
      "igoComplete": true,
      "cmoSampleIdFields": {
        "naToExtract": "",
        "sampleType": "Buffy Coat",
        "normalizedPatientId": "MRN_REDACTED",
        "recipe": "GENESET101_BAITS"
      },
      "patientAliases": [
        {
          "namespace": "cmo",
          "value": "C-TX6DNG"
        }
      ],
      "sampleAliases": [
        {
          "namespace": "igoId",
          "value": "22022_CC_3"
        },
        {
          "namespace": "investigatorId",
          "value": "LMNO_4396_N"
        }
      ],
      "additionalProperties": {
        "isCmoSample": "true",
        "igoRequestId": "IGO_TEST_REQUEST"
      }
    }
  ],
  "pooledNormals": [
    "/FASTQ/Project_POOLEDNORMALS/Sample_FFPEPOOLEDNORMAL_IGO_GENESET101_ACCGTCCT/FFPEPOOLEDNORMAL_IGO_GENESET101_ACCGTCCT_S118_R1_001.fastq.gz",
    "/FASTQ/Project_POOLEDNORMALS/Sample_FFPEPOOLEDNORMAL_IGO_GENESET101_ACCGTCCT/FFPEPOOLEDNORMAL_IGO_GENESET101_ACCGTCCT_S118_R2_001.fastq.gz",
    "/FASTQ/Project_POOLEDNORMALS/Sample_FROZENPOOLEDNORMAL_IGO_GENESET101_TGTCTAAC/FROZENPOOLEDNORMAL_IGO_GENESET101_TGTCTAAC_S22_R1_001.fastq.gz",
    "/FASTQ/Project_POOLEDNORMALS/Sample_FROZENPOOLEDNORMAL_IGO_GENESET101_TGTCTAAC/FROZENPOOLEDNORMAL_IGO_GENESET101_TGTCTAAC_S22_R2_001.fastq.gz"
  ],
  "igoProjectId": "22022"
}
`

func UnmarshalT[T any](b []byte) (T, error) {
	var target T
	if err := json.Unmarshal(b, &target); err != nil {
		return target, err
	}
	return target, nil
}
