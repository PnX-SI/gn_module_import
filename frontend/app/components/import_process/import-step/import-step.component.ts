import { Component, OnInit, ViewChild } from "@angular/core";
import { Router, ActivatedRoute } from "@angular/router";
import { StepsService, Step4Data } from "../steps.service";
import { DataService } from "../../../services/data.service";
import { CsvExportService } from "../../../services/csv-export.service";
import { CommonService } from "@geonature_common/service/common.service";
import { ModuleConfig } from "../../../module.config";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import FixModel from "../../need-fix/need-fix.models";

@Component({
    selector: "import-step",
    styleUrls: ["import-step.component.scss"],
    templateUrl: "import-step.component.html"
})
export class ImportStepComponent implements OnInit {
    public isCollapsed = false;
    public idImport: any;
    importDataRes: any;
    validData: any;
    total_columns: any;
    columns: any[] = [];
    tableReady: boolean = false;
    stepData: Step4Data;
    nValidData: number;
    nInvalidData: number;
    validBbox: any;
    public fix: FixModel
    public spinner: boolean = false;
    public nbLignes: string = "X";
    public nbError: number;
    public nbWarning: number;

    @ViewChild("modalRedir") modalRedir: any;

    constructor(
        private stepService: StepsService,
        private _csvExport: CsvExportService,
        private _router: Router,
        private _route: ActivatedRoute,
        private _ds: DataService,
        private _modalService: NgbModal,
        private _commonService: CommonService
    ) { }

    ngOnInit() {
        // set idImport from URL (email) or from localstorage
        this.idImport =
            this._route.snapshot.paramMap.get("id_import") ||
            this.stepService.getStepData(4).importId;

        this.getValidData();
        this._ds.getErrorList(this.idImport).subscribe(errorList => {
            this.nbError = errorList.errors.filter(error => error.error_level == "ERROR").length
            this.nbWarning = errorList.errors.filter(error => error.error_level == "WARNING").length
        });

    }

    openErrorSheet(idImport) {
        // this._router.navigate(["/import/errors", idImport]);
        const newRelativeUrl = this._router.createUrlTree([
            "/import/report",
            idImport
        ]);
        let baseUrl = window.location.href.replace(this._router.url, "");
        window.open(baseUrl + newRelativeUrl, "_blank");

        // <a target="_blank" [routerLink]="['/import/errors', idImport]">
    }

    onStepBack() {
        if (!ModuleConfig.ALLOW_VALUE_MAPPING) {
            this._router.navigate([`${ModuleConfig.MODULE_URL}/process/id_import/${this.idImport}/step/2`]);
        } else {
            this._router.navigate([`${ModuleConfig.MODULE_URL}/process/id_import/${this.idImport}/step/3`]);
        }
    }

    onImport() {
        this.spinner = true;
        this._ds.importData(this.idImport).subscribe(
            res => {
                console.log(res);

                this.spinner = false;

                this.stepService.resetStepoer();

                if (res.source_count > ModuleConfig.MAX_LINE_LIMIT) {
                    this.nbLignes = res.source_count;
                    this._modalService.open(this.modalRedir);
                } else this._router.navigate([`${ModuleConfig.MODULE_URL}`]);
            },
            error => {
                this.spinner = false;
                if (error.statusText === "Unknown Error") {
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
                }
            }
        );
    }

    onRedirect() {
        this._router.navigate([ModuleConfig.MODULE_URL]);
    }

    getValidData() {
        this.spinner = true;
        this._ds.getValidData(this.idImport).subscribe(
            res => {
                this.spinner = false;
                this.total_columns = res.total_columns;
                this.nValidData = res.n_valid_data;
                this.nInvalidData = res.n_invalid_data;
                this.validData = res.valid_data;
                this.validBbox = res.valid_bbox;
                this.fix = res.fix;
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
    }
}
