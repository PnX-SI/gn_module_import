import { Component, OnInit } from "@angular/core";
import { Router } from "@angular/router";
import { CommonService } from "@geonature_common/service/common.service";
import { DataService } from "../../services/data.service";
import { ModuleConfig } from "../../module.config";
import {
  Step2Data,
  Step1Data,
  Step3Data,
  Step4Data
} from "../import_process/steps.service";
import { CsvExportService } from "../../services/csv-export.service";

@Component({
  selector: "pnx-import",
  styleUrls: ["import.component.scss"],
  templateUrl: "import.component.html"
})
export class ImportComponent implements OnInit {
  public deletedStep1;
  public history;
  public empty: boolean = false;
  public config = ModuleConfig;
  historyId: any;
  n_invalid: any;
  csvDownloadResp: any;

  constructor(
    private _ds: DataService,
    private _csvExport: CsvExportService,
    private _router: Router,
    private _commonService: CommonService
  ) {}

  ngOnInit() {
    this.onImportList();
  }

  private onImportList() {
    this._ds.getImportList().subscribe(
      res => {
        this.history = res.history;
        this.empty = res.empty;
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
          this._commonService.regularToaster("error", error.error.message);
        }
      }
    );
  }

  onFinishImport(row) {
    localStorage.setItem("startPorcess", JSON.stringify(true));
    let separator = ModuleConfig.SEPARATOR.find(
      separator => separator.db_code === row.separator
    );
    let dataStep1: Step1Data = {
      importId: row.id_import,
      datasetId: row.id_dataset,
      formData: {
        fileName: row.full_file_name,
        encoding: row.encoding,
        srid: row.srid,
        separator: separator.code /// separator to convert
      }
    };
    localStorage.setItem("step1Data", JSON.stringify(dataStep1));
    let dataStep2: Step2Data = {
      importId: row.id_import,
      srid: row.srid,
      mappingIsValidate: false
    };
    switch (row.step) {
      case 2: {
        localStorage.setItem("step2Data", JSON.stringify(dataStep2));
        break;
      }
      case 3: {
        dataStep2.id_field_mapping = row.id_field_mapping;
        dataStep2.mappingRes = {};
        dataStep2.mappingRes.table_name = row.import_table;
        dataStep2.mappingRes.n_table_rows = row.source_count;
        dataStep2.mappingIsValidate = true;
        let dataStep3: Step3Data = {
          importId: row.id_import
        };
        localStorage.setItem("step2Data", JSON.stringify(dataStep2));
        localStorage.setItem("step3Data", JSON.stringify(dataStep3));
        break;
      }
      case 4: {
        dataStep2.id_field_mapping = row.id_field_mapping;
        let dataStep3: Step3Data = {
          importId: row.id_import
        };
        let dataStep4: Step4Data = {
          importId: row.id_import
        };
        dataStep2.id_field_mapping = row.id_field_mapping;
        dataStep3.id_content_mapping = row.id_content_mapping;
        localStorage.setItem("step2Data", JSON.stringify(dataStep2));
        localStorage.setItem("step3Data", JSON.stringify(dataStep3));
        localStorage.setItem("step4Data", JSON.stringify(dataStep4));
        break;
      }
    }
    this._router.navigate([
      `${ModuleConfig.MODULE_URL}/process/step/${row.step}`
    ]);
  }
}
