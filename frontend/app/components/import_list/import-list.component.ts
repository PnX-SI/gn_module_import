// @ts-ignore

import { Component, OnInit, ViewEncapsulation } from "@angular/core";
import { Router, ActivatedRoute } from "@angular/router";
import { FormControl } from "@angular/forms";
import { CommonService } from "@geonature_common/service/common.service";
import { CruvedStoreService } from '@geonature_common/service/cruved-store.service';
import { DataService } from "../../services/data.service";
import { ModuleConfig } from "../../module.config";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { ImportProcessService} from "../import_process/import-process.service";
import { Step } from "../../models/enums.model";
import { Import } from "../../models/import.model";
import { CsvExportService } from "../../services/csv-export.service";


@Component({
  styleUrls: ["import-list.component.scss"],
  templateUrl: "import-list.component.html",
})
export class ImportListComponent implements OnInit {
    public history;
    public filteredHistory;
    public empty: boolean = false;
    public config = ModuleConfig;
    public deleteOne: Import;
    public interval: any;
    public search = new FormControl()
    public total: number
    public offset: number
    public limit: number

    constructor(
        public _cruvedStore: CruvedStoreService,
        private _ds: DataService,
        private _csvExport: CsvExportService,
        private _router: Router,
        private _commonService: CommonService,
        private modal: NgbModal,
        private route: ActivatedRoute,
        private importProcessService: ImportProcessService,
    ) {
    }

    ngOnInit() {

        this.onImportList();


        this.search.valueChanges.subscribe(value => {
            this.updateFilter(value);
        });
    }

    ngOnDestroy() {
        this._ds.getImportList().subscribe().unsubscribe();
    }

    updateFilter(val: any) {
        const value = val.toString().toLowerCase().trim();

        // listes des colonnes selon lesquelles filtrer
        const cols = this.config.LIST_COLUMNS_FRONTEND.filter(item => {
            return item['filter'];
        });

        // Un resultat est retenu si au moins une colonne contient le mot-cle
        this.filteredHistory = this.history.filter(item => {
            for (let i = 0; i < cols.length; i++) {
                if (
                    (item[cols[i]['prop']] && item[cols[i]['prop']]
                        .toString()
                        .toLowerCase()
                        .indexOf(value) !== -1) ||
                    !value
                ) {
                    return true;
                }
            }
        });
    }

    private onImportList() {

        this._ds.getImportList().subscribe(
            res => {
                this.history = res["imports"];
                this.filteredHistory = this.history;
                this.empty = res.length == 0;
                this.total = res["count"]
                this.limit = res["limit"]
                this.offset = res["offset"]
            },
            error => {
                if (error.statusText === "Unknown Error") {
                    // show error message if no connexion
                    this._commonService.regularToaster(
                        "error",
                        "Une erreur s'est produite : contactez l'administrateur du site"
                    )
                } else if (error.status === 404) {
                    this._commonService.regularToaster("warning", "Aucun import trouvé");

                } else {
                    // show error message if other server error
                    this._commonService.regularToaster("error", error.error.message);
                }
            }
        );
    }

    onFinishImport(data: Import) {
        this.importProcessService.continueProcess(data);
    }

    onViewDataset(row: Import) {
        this._router.navigate([
            `metadata/dataset_detail/${row.id_dataset}`
        ]);
    }

    openDeleteModal(row: Import, modalDelete) {
        this.deleteOne = row;
        this.modal.open(modalDelete);
    }

    setPage(e) {
        this._ds.getImportList(e.offset + 1).subscribe(res => {
                this.history = res["imports"];
                this.filteredHistory = this.history;
                this.empty = res.length == 0;
                this.total = res["count"]
                this.limit = res["limit"]
                this.offset = res["offset"]
            },
            error => {
                if (error.statusText === "Unknown Error") {
                    // show error message if no connexion
                    this._commonService.regularToaster(
                        "error",
                        "Une erreur s'est produite : contactez l'administrateur du site"
                    )
                } else if (error.status === 404) {
                    this._commonService.regularToaster("warning", "Aucun import trouvé");

                } else {
                    // show error message if other server error
                    this._commonService.regularToaster("error", error.error.message);
                }
            }
        );
    };
}
