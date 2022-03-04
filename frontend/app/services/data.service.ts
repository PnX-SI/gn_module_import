import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { HttpClient, HttpHeaders } from "@angular/common/http";
import { AppConfig } from "@geonature_config/app.config";
import { ModuleConfig } from "../module.config";
import { Import, ImportError, ImportValues, SynthesisThemeFields } from "../models/import.model";
import { Mapping, MappingField, MappingContent } from "../models/mapping.model";

const urlApi = `${AppConfig.API_ENDPOINT}/${ModuleConfig.MODULE_URL}`;

@Injectable()
export class DataService {
  constructor(private _http: HttpClient) { }

  getImportList(): Observable<Array<Import>> {
    return this._http.get<Array<Import>>(`${urlApi}/imports/`);
  }

  getOneImport(id_import): Observable<Import> {
    return this._http.get<Import>(`${urlApi}/imports/${id_import}/`);
  }


  addFile(datasetId: number, file: File): Observable<Import> {
    let fd = new FormData();
    fd.append("file", file, file.name);
    fd.append("datasetId", String(datasetId));
    const url = `${urlApi}/imports/upload`;
    return this._http.post<Import>(url, fd);
  }

  updateFile(importId: number, file: File): Observable<Import> {
    let fd = new FormData();
    fd.append("file", file, file.name);
    const url = `${urlApi}/imports/${importId}/upload`;
    return this._http.put<Import>(url, fd);
  }

  decodeFile(importId: number, params: { encoding: string, format: string, srid: string}): Observable<Import> {
    const url = `${urlApi}/imports/${importId}/decode`;
    return this._http.post<Import>(url, params);
  }

  updateImport(idImport, data): Observable<Import> {
    return this._http.post<Import>(`${urlApi}/imports/${idImport}/update`, data);
  }

  createMapping(name, type): Observable<Mapping> {
    return this._http.post<Mapping>(`${urlApi}/mappings/`, {
        name: name,
        type: type,
    });
  }

  getMappings(mapping_type): Observable<Array<Mapping>> {
    return this._http.get<Array<Mapping>>(`${urlApi}/mappings/?type=${mapping_type}`);
  }

  getMapping(id_mapping): Observable<Mapping> {
    return this._http.get<Mapping>(`${urlApi}/mappings/${id_mapping}/`);
  }

  getMappingFields(id_mapping: number): Observable<Array<MappingField>> {
    return this._http.get<Array<MappingField>>(`${urlApi}/mappings/${id_mapping}/fields`);
  }

  updateMappingFields(id_mapping: number, data): Observable<Array<MappingField>> {
    return this._http.post<Array<MappingField>>(`${urlApi}/mappings/${id_mapping}/fields`, data);
  }

  getMappingContents(id_mapping: number): Observable<Array<MappingContent>> {
    return this._http.get<Array<MappingContent>>(`${urlApi}/mappings/${id_mapping}/contents`);
  }

  updateMappingContents(id_mapping: number, data): Observable<Array<MappingContent>> {
    return this._http.post<Array<MappingContent>>(`${urlApi}/mappings/${id_mapping}/contents`, data);
  }

  // Return all mappings of the same type of the removed mapping
  deleteMapping(id_mapping: number): Observable<Array<Mapping>> {
    return this._http.delete<Array<Mapping>>(`${urlApi}/mappings/${id_mapping}/`);
  }

  /**
   * Perform all data checking on the table (content et field)
   * @param idImport
   * @param idFieldMapping
   * @param idContentMapping
   */
  /*dataChecker(idImport, idFieldMapping, idContentMapping): Observable<Import> {
    const url = `${urlApi}/data_checker/${idImport}/field_mapping/${idFieldMapping}/content_mapping/${idContentMapping}`;
    return this._http.post<Import>(url, new FormData());
  }*/

  renameMapping(idMapping, name): Observable<Mapping> {
    const url = `${urlApi}/mappings/${idMapping}/name`;
    const payload = {"name": name}
    return this._http.post<Mapping>(url, payload);
  }

  deleteImport(importId: number): Observable<void> {
    return this._http.delete<void>(`${urlApi}/imports/${importId}/`);
  }

  /**
   * Return all the column of the file of an import
   * @param idImport : integer
   */
  getColumnsImport(idImport: number): Observable<Array<string>> {
    return this._http.get<Array<string>>(`${urlApi}/imports/${idImport}/columns`);
  }

  getImportValues(idImport: number): Observable<ImportValues> {
    return this._http.get<ImportValues>(`${urlApi}/imports/${idImport}/values`);
  }

  getBibFields(): Observable<Array<SynthesisThemeFields>> {
    return this._http.get<Array<SynthesisThemeFields>>(`${urlApi}/synthesis/fields`);
  }

  setImportFieldMapping(idImport: number, idFieldMapping: number): Observable<Import> {
    return this._http.post<Import>(`${urlApi}/imports/${idImport}/fieldMapping`, { id_field_mapping: idFieldMapping });
  }

  setImportContentMapping(idImport: number, idContentMapping: number): Observable<Import> {
    return this._http.post<Import>(`${urlApi}/imports/${idImport}/contentMapping`, { id_content_mapping: idContentMapping });
  }

  getNomencInfo(id_import: number) {
    return this._http.get<any>(
      `${urlApi}/imports/${id_import}/contentMapping`
    );
  }

  prepareImport(import_id: number): Observable<Import> {
    return this._http.post<Import>(`${urlApi}/imports/${import_id}/prepare`, {});
  }

  getValidData(import_id: number): Observable<any> {
    return this._http.get<any>(`${urlApi}/imports/${import_id}/preview_valid_data`);
  }

  finalizeImport(import_id): Observable<Import> {
    return this._http.post<Import>(`${urlApi}/imports/${import_id}/import`, {});
  }

  getErrorCSV(importId: number) { // FIXME
    let fd = new FormData();
    return this._http.post(`${urlApi}/imports/${importId}/invalid_rows`, fd, {
      responseType: "blob"
      //headers: new HttpHeaders().append("Content-Type", "application/json")
    });
  }

  getImportErrors(importId): Observable<Array<ImportError>> {
    return this._http.get<Array<ImportError>>(`${urlApi}/imports/${importId}/errors`);
  }
}
