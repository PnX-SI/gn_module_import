import { Injectable } from "@angular/core";
import { HttpClient, HttpHeaders } from "@angular/common/http";
import { AppConfig } from "@geonature_config/app.config";
import { ModuleConfig } from "../module.config";
import { DataFormService } from "@geonature_common/form/data-form.service";

const HttpUploadOptions = {
  headers: new HttpHeaders({ Accept: "application/json" })
};
const urlApi = `${AppConfig.API_ENDPOINT}/${ModuleConfig.MODULE_URL}`;
const GNAPI: string = AppConfig.API_ENDPOINT;

@Injectable()
export class DataService {
  constructor(private _http: HttpClient, private _gnFormService: DataFormService) { }

  getImportList() {
    return this._http.get<any>(urlApi);
  }

  getOneImport(id_import) {
    return this._http.get<any>(`${urlApi}/${id_import}`);
  }

  updateImport(idImport, data) {
    return this._http.post(`${urlApi}/update_import/${idImport}`, data);
  }

  deleteMapping(idMapping) {
    return this._http.delete(`${urlApi}/mapping/${idMapping}`);
  }

  postUserFile(value, datasetId, importId, isFileChanged, fileName) {
    const urlStatus = `${urlApi}/uploads`;
    let fd = new FormData();
    if (value.file instanceof Blob)
      fd.append("File", value.file, value.file["name"]);
    fd.append("encodage", value.encodage);
    fd.append("srid", value.srid);
    fd.append("separator", value.separator);
    fd.append("datasetId", datasetId);
    fd.append("importId", importId);
    fd.append("isFileChanged", isFileChanged);
    fd.append("fileName", fileName);
    return this._http.post<any>(urlStatus, fd, HttpUploadOptions);
  }

  getUserDatasets() {
    return this._http.get<any>(`${urlApi}/datasets`);
  }

  getMappings(mapping_type) {
    return this._http.get<any>(`${urlApi}/mappings/${mapping_type}`);
  }

  getOneBibMapping(id_mapping) {
    return this._http.get<any>(`${urlApi}/mapping/${id_mapping}`);
  }

  getMappingFields(id_mapping: number) {
    return this._http.get<any>(`${urlApi}/field_mappings/${id_mapping}`);
  }

  getMappingContents(id_mapping: number) {
    return this._http.get<any>(`${urlApi}/content_mappings/${id_mapping}`);
  }

  createOrUpdateFieldMapping(data, id_mapping) {
    return this._http.post(
      `${urlApi}/create_or_update_field_mapping/${id_mapping}`,
      data
    );
  }

  updateContentMapping(id_mapping, data) {
    return this._http.post(
      `${urlApi}/update_content_mapping/${id_mapping}`,
      data
    );
  }

  postMappingName(value, mappingType) {
    const urlMapping = `${urlApi}/mapping`;
    value["mapping_type"] = mappingType;
    return this._http.post<any>(urlMapping, value);
  }

  /**
   * Perform all data checking on the table (content et field)
   * @param idImport
   * @param idFieldMapping
   * @param idContentMapping
   */
  dataChecker(idImport, idFieldMapping, idContentMapping) {
    const url = `${urlApi}/data_checker/${idImport}/field_mapping/${idFieldMapping}/content_mapping/${idContentMapping}`;
    return this._http.post<any>(url, {});
  }

  updateMappingName(value, mappingType, idMapping) {
    const urlMapping = `${urlApi}/updateMappingName`;
    let fd = new FormData();
    fd.append("mapping_id", idMapping);
    fd.append("mapping_type", mappingType);
    fd.append("mappingName", value);
    return this._http.post<any>(urlMapping, fd, HttpUploadOptions);
  }

  cancelImport(importId: number) {
    return this._http.get<any>(`${urlApi}/cancel_import/${importId}`);
  }

  /**
   * Return all the column of the file of an import
   * @param idImport : integer
   */
  getColumnsImport(idImport) {
    return this._http.get<any>(`${urlApi}/columns_import/${idImport}`);
  }

  getSynColumnNames() {
    return this._http.get<any>(`${urlApi}/syntheseInfo`);
  }

  getBibFields() {
    return this._http.get<any>(`${urlApi}/bibFields`);
  }

  postMapping(value, importId: number, id_mapping: number, user_srid) {
    const urlMapping = `${urlApi}/mapping/${importId}/${id_mapping}`;
    let fd = new FormData();
    for (let key of Object.keys(value)) {
      fd.append(key, value[key]);
    }
    fd.append("srid", user_srid);
    return this._http.post<any>(urlMapping, fd, HttpUploadOptions);
  }

  updateMappingField(value, importId: number, id_mapping: number) {
    const urlMapping = `${urlApi}/updateMappingField/${importId}/${id_mapping}`;
    let fd = new FormData();
    for (let key of Object.keys(value)) {
      fd.append(key, value[key]);
    }
    return this._http.post<any>(urlMapping, fd, HttpUploadOptions);
  }

  contentMappingDataChecking(import_id, id_mapping) {
    if (id_mapping == null) {
      id_mapping = 0;
    }
    const contentMappingUrl = `${urlApi}/contentMapping/${import_id}/${id_mapping}`;

    return this._http.post<any>(contentMappingUrl, {});
  }

  postMetaToStep3(import_id, id_mapping, table_name) {
    let fd = new FormData();
    fd.append("import_id", import_id);
    fd.append("id_mapping", id_mapping);
    fd.append("table_name", table_name);
    return this._http.post<any>(
      `${urlApi}/postMetaToStep3`,
      fd,
      HttpUploadOptions
    );
  }

  goToStep4(import_id, id_mapping) {
    return this._http.put<any>(
      `${urlApi}/goToStep4/${import_id}/${id_mapping}`,
      {}
    );
  }

  getNomencInfo(id_import, id_field_mapping) {
    return this._http.get<any>(
      `${urlApi}/getNomencInfo/${id_import}/field_mapping/${id_field_mapping}`
    );
  }

  getNomencInfoSynchronous(id_import: number, 
                           id_field_mapping: number): Promise<{content_mapping_info: Array<Object>}> {
    // Same as NomencInfo but returns the Promise
    return this.getNomencInfo(id_import, id_field_mapping).toPromise();
  }

  importData(import_id) {
    return this._http.get<any>(`${urlApi}/importData/${import_id}`);
  }

  getValidData(importId: number) {
    return this._http.get<any>(`${urlApi}/getValidData/${importId}`);
  }

  setNeedFix(importId: number, 
             needFix?: boolean, 
             fixComment?: string) {
    let json: {need_fix?: boolean, fix_comment?: string} = {}
    if (needFix != undefined) {
      json.need_fix = needFix
    }
    if (fixComment != undefined) {
      json.fix_comment = fixComment
    }

    return this._http.put(`${urlApi}/setFix/${importId}`, json)
  }

  getErrorCSV(importId: number) {
    let fd = new FormData();
    return this._http.post(`${urlApi}/get_errors/${importId}`, fd, {
      responseType: "blob"
      //headers: new HttpHeaders().append("Content-Type", "application/json")
    });
  }

  checkInvalid(import_id) {
    return this._http.get<any>(`${urlApi}/check_invalid/${import_id}`);
  }

  getErrorList(importId) {
    return this._http.get<any>(`${urlApi}/get_error_list/${importId}`);
  }

  sendEmail(import_id) {
    return this._http.get<any>(`${urlApi}/sendemail`);
  }

  getTaxaRepartition(idSource, taxaRank) {
    let params = {
      id_source: idSource
    }
    return this._gnFormService.getTaxaDistribution(taxaRank, params)
  }

  canUpdateImport(importId) {
    return this._http.get(`${urlApi}/permissions/U/${importId}`)
  }

  getPdf(importId, mapImg, chartImg) {
    var formData = new FormData();
    // formData.append("map", new Blob([mapImg], {type: 'image/png'}), "map");
    // formData.append("chart", new Blob([chartImg], {type: 'image/png'}), "chart");
    formData.append("map", mapImg);
    formData.append("chart", chartImg);
    console.log(formData)
    return this._http.post(`${urlApi}/export_pdf/${importId}`, 
                           formData, {responseType: 'blob'});
  }
}
