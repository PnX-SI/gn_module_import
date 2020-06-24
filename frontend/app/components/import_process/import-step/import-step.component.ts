import { Component, OnInit, ViewChild } from "@angular/core";
import { Router } from "@angular/router";
import { StepsService, Step4Data, Step2Data, Step3Data } from "../steps.service";
import { DataService } from "../../../services/data.service";
import { CsvExportService } from "../../../services/csv-export.service";
import { CommonService } from "@geonature_common/service/common.service";
import { ModuleConfig } from "../../../module.config";
import * as _ from "lodash";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";

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
  validBbox: any;
  public spinner: boolean = false;
  displayErrors: boolean = false;
  displayWarnings: boolean = false;
  public nbLignes: string = "X";

  @ViewChild('modalRedir') modalRedir: any;

  constructor(
    private stepService: StepsService,
    private _csvExport: CsvExportService,
    private _router: Router,
    private _ds: DataService,
    private _modalService: NgbModal,
    private _commonService: CommonService
  ) { }

  ngOnInit() {
    this.stepData = this.stepService.getStepData(4);
    this.getValidData();
    this._ds.getErrorList(this.stepData.importId).subscribe(
      errorList => {
        if (errorList.errors.filter(error => error.error_level == 'ERROR').length > 0)
          this.displayErrors = true;
        if (errorList.errors.filter(error => error.error_level == 'WARNING').length > 0)
          this.displayWarnings = true;
      }
    );
    // this._ds.sendEmail(this.stepData.importId).subscribe(
    //   res => {
       
    //   },
    //   error => {
    //   }
    // );
  }

  onStepBack() {
    if (!ModuleConfig.ALLOW_VALUE_MAPPING) {
      this._router.navigate([`${ModuleConfig.MODULE_URL}/process/step/2`]);
    } else {
      this._router.navigate([`${ModuleConfig.MODULE_URL}/process/step/3`]);
    }
  }

  onImport() {
    this.spinner = true;
    this._ds.importData(this.stepData.importId).subscribe(
      res => {
        this.spinner = false;
        const step2: Step2Data = this.stepService.getStepData(2);
        const step3: Step3Data = this.stepService.getStepData(3);

        if (step2.id_field_mapping && step2.temporaryMapping) {
          this._ds.deleteMapping(step2.id_field_mapping).subscribe();
        }
        if (step3.id_content_mapping && step3.temporaryMapping) {
          this._ds.deleteMapping(step3.id_content_mapping).subscribe();
        }
        this.stepService.resetStepoer();
        if ((res+'').startsWith("Processing ")){
          this.nbLignes = (res+"").split(" ", 2)[1];
          this._modalService.open(this.modalRedir);
        } else
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

  onRedirect() {
    this._router.navigate([ModuleConfig.MODULE_URL]);
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
        this.validBbox = res.valid_bbox;
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
