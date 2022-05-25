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
    public search_string: string = ''
    public sort: string
    public dir: string

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

        this.onImportList(1, "");


        this.search.valueChanges.subscribe(value => {
            setTimeout(() => {
                if (value == this.search.value) {
                    this.updateFilter(value);
                }
            }, 500)
        });
    }

    ngOnDestroy() {
        this._ds.getImportList({}).subscribe().unsubscribe();
    }

    updateFilter(val: any) {
        const value = val.toString().toLowerCase().trim();
        this.onImportList(1, value)
        this.search_string = value
        // listes des colonnes selon lesquelles filtrer
    }

    private onImportList(page, search) {

        this._ds.getImportList({page:page, search:search}).subscribe(
            res => {
                this.history = res["imports"];
                this.filteredHistory = this.history;
                this.empty = res.length == 0;
                this.total = res["count"]
                this.limit = res["limit"]
                this.offset = res["offset"]
            },
            error => {
                if (error.status === 404) {
                    this._commonService.regularToaster("warning", "Aucun import trouvé");
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
    onSort(e) {
        let sort = e.sorts[0]
        let params = {page:1, search: this.search_string, sort: sort.prop, sort_dir:sort.dir}
        this._ds.getImportList(params).subscribe(res => {
            this.history = res["imports"];
            this.filteredHistory = this.history;
            this.empty = res.length == 0;
            this.total = res["count"]
            this.limit = res["limit"]
            this.offset = res["offset"]
            this.sort = sort.prop
            this.dir = sort.dir
        })
    }
    setPage(e) {
        let params = {page: e.offset + 1, search: this.search_string}
        if (this.sort) {
            params["sort"] = this.sort
        }
        if (this.dir) {
            params["sort_dir"] = this.dir
        }
        this._ds.getImportList(params)
            .subscribe(res => {
                this.history = res["imports"];
                this.filteredHistory = this.history;
                this.empty = res.length == 0;
                this.total = res["count"]
                this.limit = res["limit"]
                this.offset = res["offset"]
            },
            error => {
                if (error.status === 404) {
                    this._commonService.regularToaster("warning", "Aucun import trouvé");
                }
            }
        );
    };
}
