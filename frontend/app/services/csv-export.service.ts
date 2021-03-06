import { Injectable } from "@angular/core";
import { saveAs } from "file-saver";
import { DataService } from "./data.service";
import { CommonService } from "@geonature_common/service/common.service";

@Injectable()
export class CsvExportService {
  historyId: any;
  n_invalid: any;
  csvDownloadResp: any;

  constructor(
    private _ds: DataService,
    private _commonService: CommonService
  ) { }

  onCSV(id_import) {
    let filename = "invalid_data.csv";
    this._ds.getErrorCSV(id_import).subscribe(
      res => {
        saveAs(res, filename);
      },
      error => {
        if (error.statusText === "Unknown Error")
          this._commonService.regularToaster(
            "error",
            "Une erreur s'est produite : contactez l'administrateur du site"
          );
        else
          this._commonService.regularToaster(
            "error",
            "Une erreur s'est produite : contactez l'administrateur du site"
          );
      }
    );
  }
}
