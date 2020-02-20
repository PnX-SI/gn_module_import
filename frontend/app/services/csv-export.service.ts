import { Injectable } from "@angular/core";
import { FormGroup } from "@angular/forms";
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
  ) {}

  onCSV(id_import) {
    this._ds.checkInvalid(id_import).subscribe(
      res => {
        this.n_invalid = res;
      },
      error => {
        if (error.statusText === "Unknown Error") {
          // show error message if no connexion
          this._commonService.regularToaster(
            "error",
            "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
          );
        } else {
          // show error message if other server error
          console.log(error);
          this._commonService.regularToaster("error", error.error);
        }
      },
      () => {
        let filename = "invalid_data.csv";
        this._ds.getCSV(id_import).subscribe(
          res => {
            saveAs(res, filename);
          },
          error => {
            if (error.statusText === "Unknown Error")
              this._commonService.regularToaster(
                "error",
                "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
              );
            else
              this._commonService.regularToaster(
                "error",
                "INTERNAL SERVER ERROR when downloading csv file"
              );
          }
        );
      }
    );
  }
}
