import { Component, OnInit, ViewChild } from "@angular/core";
import { Router, ActivatedRoute } from "@angular/router";
import { ImportProcessService } from "../import-process.service";
import { DataService } from "../../../services/data.service";
import { CsvExportService } from "../../../services/csv-export.service";
import { CommonService } from "@geonature_common/service/common.service";
import { ModuleConfig } from "../../../module.config";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { Step } from "../../../models/enums.model";
import { Import } from "../../../models/import.model";

@Component({
    selector: "import-step",
    styleUrls: ["import-step.component.scss"],
    templateUrl: "import-step.component.html"
})
export class ImportStepComponent implements OnInit {
    private step: Step;
    private importData: Import;
    // public isCollapsed = false;
    // public idImport: any;
    // importDataRes: any;
    private validData: Array<any>;
    // total_columns: any;
    private columns: Array<any> = [];
    private nValidData: number;
    private nInvalidData: number;
    private validBbox: any;
    private spinner: boolean = true;
    // public nbLignes: string = "X";
    private errorCount: number;
    private warningCount: number;
    private invalidRowCount: number;
    private tableReady: boolean = true;

    @ViewChild("modalRedir") modalRedir: any;

    constructor(
      private importProcessService: ImportProcessService,
      private _csvExport: CsvExportService,
      private _router: Router,
      private _route: ActivatedRoute,
      private _ds: DataService,
      private _modalService: NgbModal,
      private _commonService: CommonService
    ) { }

    ngOnInit() {
      this.step = this._route.snapshot.data.step;
      this.importData = this.importProcessService.getImportData();
      // TODO : parallel requests, spinner
      this._ds.getImportErrors(this.importData.id_import).subscribe(errors => {
          this.errorCount = errors.filter(error => error.type.level == "ERROR").length;
          this.warningCount = errors.filter(error => error.type.level == "WARNING").length;
      });
      this._ds.getValidData(this.importData.id_import).subscribe(res => {
        this.spinner = false;
        this.nValidData = res.n_valid_data;
        this.nInvalidData = res.n_invalid_data;
        this.validData = res.valid_data;
        this.validBbox = res.valid_bbox;
        if (this.validData.length > 0) {
          this.columns = Object.keys(this.validData[0]).map(el => {
            return { prop: el, name: el };
          });
        }
      })
    }

    openErrorSheet() {
        const url = new URL(window.location.href);
        url.hash = this._router.serializeUrl(
            this._router.createUrlTree(['../errors'], {relativeTo: this._route})
        );
        window.open(url.href, "_blank");
    }

    onPreviousStep() {
      this.importProcessService.navigateToPreviousStep(this.step);
    }

    onImport() {
        this.spinner = true;
        this._ds.finalizeImport(this.importData.id_import).subscribe(
            importData => {
                console.log("import done");
                this.spinner = false;
                this.importProcessService.setImportData(importData);
                /*if (res.source_count > ModuleConfig.MAX_LINE_LIMIT) {
                    this.nbLignes = res.source_count;
                    this._modalService.open(this.modalRedir);
                } else this._router.navigate([`${ModuleConfig.MODULE_URL}`]);*/
            },
            error => {
                console.log("import error");
                this.spinner = false;
                this._commonService.regularToaster("error", error.error.description);

                /*if (error.statusText === "Unknown Error") {
                    // show error message if no connexion
                    this._commonService.regularToaster(
                        "error",
                        "Une erreur s'est produite : contactez l'administrateur du site"
                    );
                } else {
                    // show error message if other server error
                    this._commonService.regularToaster(
                        "error",
                        error.error.message + " = " + error.error.details
                    );
                }*/
            }
        );
    }

    /*onRedirect() {
        this._router.navigate([ModuleConfig.MODULE_URL]);
    }*/

    /*getValidData() {
        this.spinner = true;
        this._ds.getValidData(this.idImport).subscribe(
            res => {
                this.spinner = false;
                this.total_columns = res.total_columns;
                this.nValidData = res.n_valid_data;
                this.nInvalidData = res.n_invalid_data;
                this.validData = res.valid_data;
                this.validBbox = res.valid_bbox;
                this.columns = [];

                if (this.validData.length > 0) {
                    this.columns = Object.keys(this.validData[0]).map(el => {
                        return { prop: el, name: el }

                    });
                }
                this.tableReady = true;

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
    }*/
}
