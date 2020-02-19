import { Component, OnInit } from "@angular/core";
import { Router } from "@angular/router";
import { StepsService, Step4Data } from "../steps.service";
import { DataService } from "../../../services/data.service";
import { CsvExportService } from "../../../services/csv-export.service";
import { CommonService } from "@geonature_common/service/common.service";
import { ModuleConfig } from "../../../module.config";
import * as _ from "lodash";

@Component({
  selector: "import-step",
  styleUrls: ["import-step.component.scss"],
  templateUrl: "import-step.component.html"
})
export class ImportStepComponent implements OnInit {
  public isCollapsed = false;
  importDataRes: any;
  validData: any;
  total_columns: any;
  columns: any[] = [];
  rows: any[] = [];
  tableReady: boolean = false;
  stepData: Step4Data;
  nValidData: number;
  nInvalidData: number;
  public spinner: boolean = false;

  constructor(
    private stepService: StepsService,
    private _csvExport: CsvExportService,
    private _router: Router,
    private _ds: DataService,
    private _commonService: CommonService
  ) {}

  ngOnInit() {
    this.stepData = this.stepService.getStepData(4);
    this.getValidData();
  }

  onStepBack() {
    this._router.navigate([`${ModuleConfig.MODULE_URL}/process/step/3`]);
  }

  onImport() {
    this.spinner = true;
    this._ds.importData(this.stepData.importId).subscribe(
      res => {
        this.spinner = false;
        this.importDataRes = res;
        this.stepService.resetStepoer();
        this._router.navigate([`${ModuleConfig.MODULE_URL}`]);
      },
      error => {
        this.spinner = false;
        if (error.statusText === "Unknown Error") {
          // show error message if no connexion
          this._commonService.regularToaster(
            "error",
            "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
          );
        } else {
          // show error message if other server error
          this._commonService.regularToaster(
            "error",
            error.error.message + " = " + error.error.details
          );
        }
      }
    );
  }

  getValidData() {
    this.spinner = true;
    this._ds.getValidData(this.stepData.importId).subscribe(
      res => {
        this.spinner = false;
        this.total_columns = res.total_columns;
        this.nValidData = res.n_valid_data;
        this.nInvalidData = res.n_invalid_data;
        this.validData = res.valid_data;
        if (this.validData != "no data") {
          this.columns = [];
          this.rows = [];
          _.forEach(this.validData[0], el => {
            let key = el.key;
            let val = el.value;
            this.columns.push({ name: key, prop: key });
          });

          _.forEach(this.validData, data => {
            let obj = {};
            _.forEach(data, el => {
              let key = el.key;
              let val = el.value;
              obj[key] = val;
            });
            this.rows.push(obj);
          });

          this.tableReady = true;
        }
      },
      error => {
        this.spinner = false;
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
}
