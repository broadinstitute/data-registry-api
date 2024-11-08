import io
import json
import re

import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_s3, mock_batch
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY, HTTP_200_OK, HTTP_404_NOT_FOUND, HTTP_401_UNAUTHORIZED, \
    HTTP_400_BAD_REQUEST

from dataregistry.api.model import DataFormat, User, HermesFileStatus
from dataregistry.api.jwt import get_encoded_jwt_data

AUTHORIZATION = "Authorization"

auth_token = f"Bearer {get_encoded_jwt_data(User(user_name='test', roles=['admin'], id=1))}"
view_only_token = f"Bearer {get_encoded_jwt_data(User(user_name='view', roles=['viewer'], id=2))}"

dataset_api_path = '/api/datasets'
study_api_path = '/api/studies'

example_study_json = {
    "name": "Test Study",
    "institution": "UCSF"
}

example_dataset_json = {
    "name": "Cade2021_SleepApnea_Mixed_Female",
    "data_source_type": "file",
    "data_type": "wgs",
    "genome_build": "hg19",
    "ancestry": "EA",
    "data_submitter": "Jennifer Doudna",
    "data_submitter_email": "researcher@institute.org",
    "sex": "female",
    "global_sample_size": 11,
    "status": "open",
    "description": "Lorem ipsum..",
    "publicly_available": False
}


def test_get_datasets(api_client: TestClient):
    response = api_client.get(dataset_api_path, headers={AUTHORIZATION: auth_token})
    assert response.status_code == HTTP_200_OK
    assert len(response.json()) == 0


@mock_s3
def test_post_dataset(api_client: TestClient):
    create_new_dataset(api_client)


@mock_s3
def test_update_dataset(api_client: TestClient):
    copy, new_ds_id = create_new_dataset(api_client)
    copy.update({'id': new_ds_id, 'name': 'Updated Dataset Name'})
    response = api_client.patch(dataset_api_path, headers={AUTHORIZATION: auth_token}, json=copy)
    assert response.status_code == HTTP_200_OK
    response = api_client.get(f"{dataset_api_path}/{new_ds_id}", headers={AUTHORIZATION: auth_token})
    assert response.json()['dataset']['name'] == 'Updated Dataset Name'


@mock_s3
def test_update_dataset_fk_failure(api_client: TestClient):
    copy, new_ds_id = create_new_dataset(api_client)
    copy.update({'id': new_ds_id, 'study_id': 'missing_id'})
    response = api_client.patch(f"{dataset_api_path}", headers={AUTHORIZATION: auth_token}, json=copy)
    assert response.status_code == HTTP_400_BAD_REQUEST


def create_new_dataset(api_client, ds_info=example_dataset_json):
    set_up_moto_bucket()
    copy = ds_info.copy()
    if 'study_id' not in copy:
        study_id = save_study(api_client)
        copy.update({'study_id': study_id})
    create_response = api_client.post(dataset_api_path, headers={AUTHORIZATION: auth_token}, json=copy)
    assert create_response.status_code == HTTP_200_OK
    new_ds_id = create_response.json()['id']
    return copy, new_ds_id


def save_study(api_client):
    response = api_client.post(study_api_path, headers={AUTHORIZATION: auth_token}, json=example_study_json)
    assert response.status_code == HTTP_200_OK
    study_id = response.json()['id'].replace('-', '')
    return study_id


def set_up_moto_bucket():
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="dig-data-registry")


@mock_s3
def test_post_then_retrieve_by_id(api_client: TestClient):
    set_up_moto_bucket()
    new_dataset = example_dataset_json.copy()
    study_id = save_study(api_client)
    new_dataset.update({'study_id': study_id, 'name': 'to-retrieve'})
    copy, new_ds_id = create_new_dataset(api_client, new_dataset)
    response = api_client.get(f"{dataset_api_path}/{new_ds_id}", headers={AUTHORIZATION: auth_token})
    assert response.status_code == HTTP_200_OK


@mock_s3
def test_upload_file(api_client: TestClient):
    new_record = add_ds_with_file(api_client)
    s3_conn = boto3.resource("s3", region_name="us-east-1")
    file_text = s3_conn.Object("dig-data-registry", f"{new_record['dataset']['name']}/t1d/sample_upload.txt").get()[
        "Body"].read() \
        .decode("utf-8")
    assert file_text == "The answer is 47!\n"


@mock_s3
def test_uploaded_file_is_not_public(api_client: TestClient):
    new_record = add_ds_with_file(api_client)
    response = api_client.get(f"/api/d/{new_record['phenotypes'][0]['short_id']}",
                              headers={AUTHORIZATION: auth_token})
    assert response.status_code == HTTP_404_NOT_FOUND


@mock_s3
def test_list_files(api_client: TestClient):
    new_record = add_ds_with_file(api_client, public=True)
    response = api_client.get(f"/api/filelist/{new_record['dataset']['id']}", headers={AUTHORIZATION: auth_token})
    assert response.status_code == HTTP_200_OK
    result = response.json()[0]
    assert re.match(r'd/[a-zA-Z0-9]{6}', result['path']) is not None


def add_ds_with_file(api_client, public=False):
    new_record = example_dataset_json.copy()
    record_name = 'file_upload_test'
    study_id = save_study(api_client)
    new_record.update({'study_id': study_id, 'name': record_name})
    if public:
        new_record.update({'publicly_available': True})
    copy, new_dataset_id = create_new_dataset(api_client, ds_info=new_record)
    with open("tests/sample_upload.txt", "rb") as f:
        upload_response = api_client.post(f"/api/uploadfile/{new_dataset_id.replace('-', '')}/true/10?phenotype=t1d",
                                          headers={AUTHORIZATION: auth_token, "Filename": "sample_upload.txt"},
                                          files={"file": f})
        assert upload_response.status_code == HTTP_200_OK
    new_record = api_client.get(f"/api/datasets/{new_dataset_id}", headers={AUTHORIZATION: auth_token}).json()
    return new_record


@mock_s3
def test_upload_credible_set(api_client: TestClient):
    ds = add_ds_with_file(api_client, public=True)
    with open("tests/sample_upload.txt", "rb") as f:
        credible_set_name = "credible_set"
        upload_response = api_client.post(
            f"/api/crediblesetupload/{str(ds['phenotypes'][0]['id']).replace('-', '')}/{credible_set_name}",
            headers={AUTHORIZATION: auth_token, "Filename": "sample_upload.txt"},
            files={"file": f})
        assert upload_response.status_code == HTTP_200_OK
    saved_dataset = api_client.get(f"{dataset_api_path}/{ds['dataset']['id']}", headers={AUTHORIZATION: auth_token})
    json = saved_dataset.json()
    credible_sets = json['credible_sets']
    assert len(credible_sets) == 1


@pytest.mark.parametrize("df", DataFormat.__members__.values())
@mock_s3
def test_valid_data_formats_post(api_client: TestClient, df: DataFormat):
    new_record = example_dataset_json.copy()
    new_record['data_type'] = df
    create_new_dataset(api_client, new_record)


def test_invalid_record_post(api_client: TestClient):
    new_record = example_dataset_json.copy()
    new_record['ancestry'] = 'bad-ancestry'
    response = api_client.post(dataset_api_path, headers={AUTHORIZATION: auth_token}, json=new_record)
    assert response.status_code == HTTP_422_UNPROCESSABLE_ENTITY


@mock_s3
def test_delete_dataset(api_client: TestClient):
    ds_with_file = add_ds_with_file(api_client)['dataset']
    ds_id = ds_with_file['id']
    del_response = api_client.delete(f"{dataset_api_path}/{ds_id}", headers={AUTHORIZATION: auth_token})
    assert del_response.status_code == HTTP_200_OK
    saved_dataset_response = api_client.get(f"{dataset_api_path}/{ds_id}", headers={AUTHORIZATION: auth_token})
    assert saved_dataset_response.status_code == HTTP_404_NOT_FOUND


@mock_s3
def test_delete_dataset_without_auth(api_client: TestClient):
    ds_with_file = add_ds_with_file(api_client)['dataset']
    ds_id = ds_with_file['id']
    response = api_client.delete(f"{dataset_api_path}/{ds_id}")
    assert response.status_code == HTTP_401_UNAUTHORIZED


@mock_s3
def test_delete_without_access(api_client: TestClient):
    ds_with_file = add_ds_with_file(api_client)['dataset']
    ds_id = ds_with_file['id']
    response = api_client.delete(f"{dataset_api_path}/{ds_id}", headers={AUTHORIZATION: view_only_token})
    assert response.status_code == HTTP_401_UNAUTHORIZED


def test_preview_delimited_file(api_client: TestClient):
    with open('tests/test_csv_upload.csv', mode='rb') as f:
        res = api_client.post('api/preview-delimited-file', headers={AUTHORIZATION: auth_token},
                              files={'file': f})
        assert res.json() == {'columns': ["ID","CHR","BP","OA","EA","EAF","BETA","SE","P","EUR_EAF","SNP"]}


@mock_s3
@mock_batch
def test_start_meta_analysis(mocker, api_client: TestClient):
    set_up_moto_bucket()
    patch = mocker.patch('dataregistry.api.batch.submit_and_await_job')
    patch.return_value = None
    mocker.patch('boto3.client').return_value.generate_presigned_url.return_value = 'http://mocked-presigned-url'

    mock_aiohttp_put = mocker.patch('aiohttp.ClientSession.put')
    mock_aiohttp_put.return_value.__aenter__.return_value.status = 200
    with open('tests/test_csv_upload.csv', mode='rb') as f:
        file_bytes = f.read()

    mock_s3_get_object = mocker.patch('boto3.client').return_value.get_object
    mock_s3_get_object.return_value = {
        'Body': io.BytesIO(file_bytes),
        'ContentLength': len(file_bytes)
    }
    res = api_client.get('api/validate-hermes', headers={AUTHORIZATION: auth_token, 'Filename': 'foo.csv',
                                                         'Dataset': 'unit-test-dataset',
                                                         'Metadata': json.dumps({'b': 1, 'phenotype': 'T2D',
                                                                                 'column_map': {"chromosome": "CHR",
                                                                                                "position": "BP",
                                                                                                "eaf": "EAF",
                                                                                                "beta": "BETA",
                                                                                                "se": "SE",
                                                                                                "pValue": "P"}})})
    result_dict = res.json()
    file_id = result_dict.get("file_id")
    res = api_client.post('api/hermes-meta-analysis', headers={AUTHORIZATION: auth_token}, json={'method': 'intake',
                                                                                           'datasets': [file_id],
                                                                                           'name': 'Test Metadata',
                                                                                           'phenotype': 'T2D',
                                                                                           'created_by': 'dhite'})
    assert "meta-analysis-id" in res.json()
    res = api_client.get('api/hermes-meta-analysis', headers={AUTHORIZATION: auth_token})
    ma_results = res.json()
    assert ma_results[0].get("name") == "Test Metadata"
    assert ma_results[0].get("dataset_names") == ['unit-test-dataset']

@mock_s3
@mock_batch
def test_upload_hermes_csv(mocker, api_client: TestClient):
    set_up_moto_bucket()
    patch = mocker.patch('dataregistry.api.batch.submit_and_await_job')
    patch.return_value = None
    mocker.patch('boto3.client').return_value.generate_presigned_url.return_value = 'http://mocked-presigned-url'

    mock_aiohttp_put = mocker.patch('aiohttp.ClientSession.put')
    mock_aiohttp_put.return_value.__aenter__.return_value.status = 200
    with open('tests/test_csv_upload.csv', mode='rb') as f:
        file_bytes = f.read()

    mock_s3_get_object = mocker.patch('boto3.client').return_value.get_object
    mock_s3_get_object.return_value = {
        'Body': io.BytesIO(file_bytes),
        'ContentLength': len(file_bytes)
    }
    res = api_client.get('api/validate-hermes', headers={AUTHORIZATION: auth_token, 'Filename': 'foo.csv',
                                                         'Dataset': 'unit-test-dataset',
                                                         'Metadata': json.dumps({'b': 1, 'phenotype': 'T2D',
                                                                                 'column_map': {"chromosome": "CHR",
                                                                                                "position": "BP",
                                                                                                "eaf": "EAF",
                                                                                                "beta": "BETA",
                                                                                                "se": "SE",
                                                                                                "pValue": "P"}})})
    result_dict = res.json()
    assert "file_size" in result_dict
    assert "s3_path" in result_dict
    assert "file_id" in result_dict
    file_id = result_dict.get("file_id")
    file_details = api_client.get(f'api/upload-hermes/{file_id}', headers={AUTHORIZATION: auth_token})
    assert file_details.status_code == HTTP_200_OK
    assert file_details.json()["metadata"]["b"] == 1
    file_uploads = api_client.get('api/upload-hermes/', headers={AUTHORIZATION: auth_token}).json()
    assert len(file_uploads) == 1
    assert file_uploads[0]['metadata']['column_map']
    file_uploads = api_client.get('api/upload-hermes?uploader=dhite', headers={AUTHORIZATION: auth_token}).json()
    assert len(file_uploads) == 0
    file_uploads = api_client.get('api/upload-hermes?uploader=test', headers={AUTHORIZATION: auth_token}).json()
    assert len(file_uploads) == 1
    file_uploads = api_client.get(f"api/upload-hermes?statuses={HermesFileStatus.FAILED_QC}",
                                  headers={AUTHORIZATION: auth_token}).json()
    assert len(file_uploads) == 0
    file_uploads = api_client.get(f"api/upload-hermes?phenotype=T2D", headers={AUTHORIZATION: auth_token}).json()
    assert len(file_uploads) == 1
    file_uploads = api_client.get(f"api/upload-hermes?phenotype=T1D", headers={AUTHORIZATION: auth_token}).json()
    assert len(file_uploads) == 0
    file_uploads = api_client.get(f"api/upload-hermes?phenotype=T2D&limit=10&offset=0",
                                  headers={AUTHORIZATION: auth_token}).json()
    assert len(file_uploads) == 1


@mock_s3
def test_upload_csv(api_client: TestClient):
    set_up_moto_bucket()
    with open('tests/test_csv_upload.csv', mode='rb') as f:
        response = api_client.post('api/upload-csv', headers={AUTHORIZATION: auth_token, "Filename": "unit-test.csv"},
                                   files={"file": f})
        assert response.status_code == HTTP_200_OK
        assert "file_size" in response.json()


@mock_s3
def test_delete_phenotype(api_client: TestClient):
    ds_info = add_ds_with_file(api_client)
    p_id = ds_info['phenotypes'][0]['id']
    response = api_client.delete(f'api/phenotypes/{str(p_id).replace("-", "")}',
                                 headers={AUTHORIZATION: auth_token})
    assert response.status_code == HTTP_200_OK




def test_api_publications(mocker, api_client: TestClient):
    mock_response_publication = mocker.patch('requests.get')
    mock_response_publication.return_value.status_code = 200
    mock_response_publication.return_value.text = """<?xml version="1.0" ?>
<!DOCTYPE PubmedArticleSet PUBLIC "-//NLM//DTD PubMedArticle, 1st January 2024//EN" "https://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_240101.dtd">
<PubmedArticleSet>
<PubmedArticle><MedlineCitation Status="MEDLINE" Owner="NLM" IndexingMethod="Automated"><PMID Version="1">37278239</PMID><DateCompleted><Year>2023</Year><Month>06</Month><Day>07</Day></DateCompleted>
<DateRevised><Year>2023</Year><Month>09</Month><Day>26</Day></DateRevised><Article PubModel="Print-Electronic"><Journal><ISSN IssnType="Electronic">1473-5857</ISSN><JournalIssue CitedMedium="Internet">
<Volume>38</Volume><Issue>4</Issue><PubDate><Year>2023</Year><Month>Jul</Month><Day>01</Day></PubDate></JournalIssue>
<Title>International clinical psychopharmacology</Title><ISOAbbreviation>Int Clin Psychopharmacol</ISOAbbreviation></Journal>
<ArticleTitle>Debated issues in major psychoses.</ArticleTitle><Pagination><StartPage>201</StartPage><EndPage>203</EndPage>
<MedlinePgn>201-203</MedlinePgn></Pagination><ELocationID EIdType="doi" ValidYN="Y">10.1097/YIC.0000000000000478</ELocationID><AuthorList CompleteYN="Y">
<Author ValidYN="Y"><LastName>Serretti</LastName><ForeName>Alessandro</ForeName><Initials>A</Initials><AffiliationInfo>
<Affiliation>Department of Biomedical and Neuromotor Sciences, University of Bologna, Bologna, Italy.</Affiliation></AffiliationInfo>
</Author></AuthorList><Language>eng</Language><PublicationTypeList><PublicationType UI="D016421">Editorial</PublicationType></PublicationTypeList>
<ArticleDate DateType="Electronic"><Year>2023</Year><Month>05</Month><Day>26</Day></ArticleDate></Article><MedlineJournalInfo><Country>England</Country>
<MedlineTA>Int Clin Psychopharmacol</MedlineTA><NlmUniqueID>8609061</NlmUniqueID><ISSNLinking>0268-1315</ISSNLinking>
</MedlineJournalInfo><CitationSubset>IM</CitationSubset><MeshHeadingList><MeshHeading><DescriptorName UI="D006801" MajorTopicYN="N">Humans
</DescriptorName></MeshHeading><MeshHeading><DescriptorName UI="D011618" MajorTopicYN="Y">Psychotic Disorders</DescriptorName><QualifierName UI="Q000175" MajorTopicYN="N">diagnosis</QualifierName>
<QualifierName UI="Q000188" MajorTopicYN="N">drug therapy</QualifierName></MeshHeading></MeshHeadingList></MedlineCitation><PubmedData><History>
<PubMedPubDate PubStatus="medline"><Year>2023</Year><Month>6</Month><Day>7</Day><Hour>6</Hour><Minute>42</Minute></PubMedPubDate>
<PubMedPubDate PubStatus="pubmed"><Year>2023</Year><Month>6</Month><Day>6</Day><Hour>6</Hour><Minute>42</Minute></PubMedPubDate>
<PubMedPubDate PubStatus="entrez"><Year>2023</Year><Month>6</Month><Day>6</Day><Hour>5</Hour><Minute>27</Minute></PubMedPubDate></History>
<PublicationStatus>ppublish</PublicationStatus><ArticleIdList><ArticleId IdType="pubmed">37278239</ArticleId>
<ArticleId IdType="doi">10.1097/YIC.0000000000000478</ArticleId><ArticleId IdType="pii">00004850-202307000-00001</ArticleId>
</ArticleIdList><ReferenceList><Reference>
<Citation>Albert U, Marazziti D, Di Salvo G, Solia F, Rosso G, Maina G (2018). A systematic review of evidence-based treatment strategies for obsessive-compulsive disorder resistant to first-line pharmacotherapy. Curr Med Chem 25:5647&#x2013;5661.</Citation></Reference><Reference><Citation>Bahji A, Ermacora D, Stephenson C, Hawken ER, Vazquez G (2020). Comparative efficacy and tolerability of pharmacological treatments for the treatment of acute bipolar depression: a systematic review and network meta-analysis. J Affect Disord 269:154&#x2013;184.</Citation></Reference><Reference><Citation>Baldessarini RJ, V&#xe1;zquez GH, Tondo L (2020). Bipolar depression: a major unsolved challenge. Int J Bipolar Disord 8:1.</Citation></Reference><Reference><Citation>Bandelow B, Allgulander C, Baldwin DS, Costa DLDC, Denys D, Dilbaz N, et al. (2023). World Federation of Societies of Biological Psychiatry (WFSBP) guidelines for treatment of anxiety, obsessive-compulsive and posttraumatic stress disorders &#x2013; Version 3. Part II: OCD and PTSD. World J Biol Psychiatry 24:118&#x2013;134.</Citation></Reference><Reference><Citation>Cai L, Chen G, Yang H, Bai Y (2023). Efficacy and safety profiles of mood stabilizers and antipsychotics for bipolar depression: a systematic review. Int Clin Psychopharmacol doi:10.1097/YIC.0000000000000449.</Citation><ArticleIdList><ArticleId IdType="doi">10.1097/YIC.0000000000000449</ArticleId></ArticleIdList></Reference><Reference><Citation>Cellini L, De Donatis D, Mercolini L, Panariello F, De Ronchi D, Serretti A, et al. (2021). Switch to 3-month long-acting injectable paliperidone may decrease plasma levels: a case series. J Clin Psychopharmacol 41:694&#x2013;696.</Citation></Reference><Reference><Citation>Cetin Erdem H, Kara H, Ozcelik O, Donmez L, Eryilmaz M, Ozbey G (2023). Frequency, risk factors, and impacts on quality of life of the restless legs syndrome and side effects among antidepressant users in a tertiary hospital: an observational cross-sectional study. Int Clin Psychopharmacol doi:10.1097/YIC.0000000000000466.</Citation><ArticleIdList><ArticleId IdType="doi">10.1097/YIC.0000000000000466</ArticleId></ArticleIdList></Reference><Reference><Citation>Colom F, Lam D (2005). Psychoeducation: improving outcomes in bipolar disorder. Eur Psychiatry 20:359&#x2013;364.</Citation></Reference><Reference><Citation>Colom F, Vieta E, Reinares M, Mart&#xed;nez-Ar&#xe1;n A, Torrent C, Goikolea JM, et al. (2003). Psychoeducation efficacy in bipolar disorders: beyond compliance enhancement. J Clin Psychiatry 64:1101&#x2013;1105.</Citation></Reference><Reference><Citation>Del Casale A, Sorice S, Padovano A, Simmaco M, Ferracuti S, Lamis DA, et al. (2019). Psychopharmacological treatment of obsessive-compulsive disorder (OCD). Curr Neuropharmacol 17:710&#x2013;736.</Citation></Reference><Reference><Citation>Duarte-Silva E, Filho AJMC, Barichello T, Quevedo J, Macedo D, Peixoto C (2020). Phosphodiesterase-5 inhibitors: shedding new light on the darkness of depression? J Affect Disord 264:138&#x2013;149.</Citation></Reference><Reference><Citation>Grant JE, Hook R, Valle S, Chesivoir E, Chamberlain SR (2021). Tolcapone in obsessive-compulsive disorder: a randomized double-blind placebo-controlled crossover trial. Int Clin Psychopharmacol 36:225&#x2013;229.</Citation></Reference><Reference><Citation>Kadakia A, Dembek C, Heller V, Singh R, Uyei J, Hagi K, et al. (2021). Efficacy and tolerability of atypical antipsychotics for acute bipolar depression: a network meta-analysis. BMC Psychiatry 21:249.</Citation></Reference><Reference><Citation>Kane JM, Chen A, Lim S, Mychaskiw MA, Tian M, Wang Y, et al. (2023). Early versus late administration of long-acting injectable antipsychotic agents among patients with newly diagnosed schizophrenia: an analysis of a commercial claims database. Int Clin Psychopharmacol doi:10.1097/YIC.0000000000000452.</Citation><ArticleIdList><ArticleId IdType="doi">10.1097/YIC.0000000000000452</ArticleId></ArticleIdList></Reference><Reference><Citation>Kocamer &#x15e;ahin S, Demir B, Altinda&#x11f; A (2023). Remission of treatment-resistant obsessive-compulsive disorder with 600 milligrams of fluvoxamine daily: a case report. Int Clin Psychopharmacol doi:10.1097/YIC.0000000000000458.</Citation><ArticleIdList><ArticleId IdType="doi">10.1097/YIC.0000000000000458</ArticleId></ArticleIdList></Reference><Reference><Citation>Kolla BP, Mansukhani MP, Bostwick JM (2018). The influence of antidepressants on restless legs syndrome and periodic limb movements: a systematic review. Sleep Med Rev 38:131&#x2013;140.</Citation></Reference><Reference><Citation>Koran LM, Hanna GL, Hollander E, Nestadt G, Simpson HB; American Psychiatric Association (2007). Practice guideline for the treatment of patients with obsessive-compulsive disorder. Am J Psychiatry 164 (Suppl 7):5&#x2013;53.</Citation></Reference><Reference><Citation>Levenberg K, Cordner ZA (2022). Bipolar depression: a review of treatment options. Gen Psychiatry 35:e100760.</Citation></Reference><Reference><Citation>Malhi GS, Bell E, Boyce P, Bassett D, Berk M, Bryant R, et al. (2020). The 2020 Royal Australian and New Zealand College of psychiatrists clinical practice guidelines for mood disorders: bipolar disorder summary. Bipolar Disord 22:805&#x2013;821.</Citation></Reference><Reference><Citation>Miklowitz DJ, Efthimiou O, Furukawa TA, Scott J, McLaren R, Geddes JR, et al. (2021). Adjunctive psychotherapy for bipolar disorder: a systematic review and component network meta-analysis. JAMA Psychiatry 78:141&#x2013;150.</Citation></Reference><Reference><Citation>Mowla A, Baniasadipour H (2022). Is mirtazapine augmentation effective for patients with obsessive-compulsive disorder who failed to respond to sertraline monotherapy? A placebo-controlled, double-blind, clinical trial. Int Clin Psychopharmacol doi:10.1097/YIC.0000000000000415.</Citation><ArticleIdList><ArticleId IdType="doi">10.1097/YIC.0000000000000415</ArticleId></ArticleIdList></Reference><Reference><Citation>Murthy VS, Mangot AG (2015). Psychiatric aspects of phosphodiesterases: an overview. Indian J Pharmacol 47:594&#x2013;599.</Citation></Reference><Reference><Citation>Nawras M, Beran A, Yazdi V, Hecht M, Lewis C (2023). Phosphodiesterase inhibitor and selective serotonin reuptake inhibitor combination therapy versus monotherapy for the treatment of major depressive disorder: a systematic review and meta-analysis. Int Clin Psychopharmacol doi:10.1097/YIC.0000000000000457.</Citation><ArticleIdList><ArticleId IdType="doi">10.1097/YIC.0000000000000457</ArticleId></ArticleIdList></Reference><Reference><Citation>Patel M, Jain R, Tohen M, Maletic V, Earley WR, Yatham LN (2021). Efficacy of cariprazine in bipolar I depression across patient characteristics: a post hoc analysis of pooled randomized, placebo-controlled studies. Int Clin Psychopharmacol 36:76&#x2013;83.</Citation></Reference><Reference><Citation>Poyurovsky M, Braverman L, Weizman A (2021). Beneficial effect of quetiapine monotherapy in patients with bipolar depression and comorbid obsessive-compulsive disorder. Int Clin Psychopharmacol 36:50&#x2013;53.</Citation></Reference><Reference><Citation>Stahl SM (2014). Long-acting injectable antipsychotics: shall the last be first? CNS Spectr 19:3&#x2013;5.</Citation></Reference><Reference><Citation>Stevens GL, Dawson G, Zummo J (2016). Clinical benefits and impact of early use of long-acting injectable antipsychotics for schizophrenia. Early Interv Psychiatry 10:365&#x2013;377.</Citation></Reference><Reference><Citation>Yatham LN, Kennedy SH, Parikh SV, Schaffer A, Bond DJ, Frey BN, et al. (2018). Canadian Network for Mood and Anxiety Treatments (CANMAT) and International Society for Bipolar Disorders (ISBD) 2018 guidelines for the management of patients with bipolar disorder. Bipolar Disord 20:97&#x2013;170.</Citation></Reference><Reference><Citation>Yatham LN, Vieta E, Earley W (2020). Evaluation of cariprazine in the treatment of bipolar I and II depression: a randomized, double-blind, placebo-controlled, phase 2 trial. Int Clin Psychopharmacol 35:147&#x2013;156.</Citation></Reference></ReferenceList></PubmedData></PubmedArticle></PubmedArticleSet>
    
    """

    response = api_client.get('api/publications?pub_id=123456', headers={AUTHORIZATION: auth_token})

    assert response.status_code == 200
    assert response.json()['title'] == 'Debated issues in major psychoses.'
