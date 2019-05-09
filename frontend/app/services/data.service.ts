import { OnInit } from '@angular/core';
import { Injectable } from '@angular/core';
import { HttpClient,HttpHeaders } from '@angular/common/http';
import { AppConfig } from '@geonature_config/app.config';
import { CommonService } from '@geonature_common/service/common.service';
import { ModuleConfig } from "../module.config";

const HttpUploadOptions = {
    headers: new HttpHeaders({ "Accept": "application/json" })
  }
@Injectable()
export class DataService {

    fd: any;
    public IMPORT_CONFIG = ModuleConfig;

    constructor(
        private _http: HttpClient, 
        private _commonService: CommonService
    ) {}

    getImportList() {
        return this._http.get<any>(`${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}`);
    }

    postUserFile2(value,datasetId) {
        console.log(value);
        const urlStatus = `${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}/uploads`;
        this.fd = new FormData();
        this.fd.append('File', value.file, value.file['name']);
        this.fd.append('encodage', value.encodage);
        this.fd.append('srid', value.srid);
        this.fd.append('datasetId', datasetId);
        console.log(this.fd);
        return this._http.post<any>(urlStatus, this.fd, HttpUploadOptions);
    }

    /*
    postUserFile(selectedFile,selectedFileName) {
        const urlStatus = `${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}/uploads`;
        this.fd = new FormData();
        this.fd.append('File', selectedFile, selectedFileName);
        this.fd.set('username', 'Chris');
        console.log(this.fd);
        return this._http.post<any>(urlStatus, this.fd);
    }
    */

    /*
    postDataset(datasetId:number) {
        const url = `${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}/init`;
        return this._http.post<any>(url,datasetId);
    }
    */
    

    getUserDatasets() {
        return this._http.get<any>(`${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}/datasets`);
    }

    /*
    deleteImport(importId:number) {
        return this._http.get<any>(`${AppConfig.API_ENDPOINT}/${this.IMPORT_CONFIG.MODULE_URL}/ImportDelete/${importId}`);
    }
    */

}