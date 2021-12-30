import { Component, OnInit, ViewEncapsulation } from "@angular/core";
import { Router } from "@angular/router";
import { FormControl } from "@angular/forms";
import { CommonService } from "@geonature_common/service/common.service";
import { CruvedStoreService } from '@geonature_common/service/cruved-store.service';
import { DataService } from "../../services/data.service";
import { ModuleConfig } from "../../module.config";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
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
  templateUrl: "import.component.html",
  // encapsulation: ViewEncapsulation.None
})
export class ImportComponent implements OnInit {
  public deletedStep1;
  public history;
  public filteredHistory;
  public empty: boolean = false;
  public config = ModuleConfig;
  historyId: any;
  n_invalid: any;
  csvDownloadResp: any;
  public deleteOne: any;
  public interval: any;

  public search = new FormControl()


  constructor(
    public _cruvedStore: CruvedStoreService,
    private _ds: DataService,
    private _csvExport: CsvExportService,
    private _router: Router,
    private _commonService: CommonService,
    private modal: NgbModal
  ) { }

  ngOnInit() {

    this.onImportList();

    clearInterval(this.interval)
    this.interval = setInterval(() => {
      this.onImportList();
    }, 15000);

    this.search.valueChanges.subscribe(value => {
      this.updateFilter(value);
    });
  }

  ngOnDestroy() {
    this._ds.getImportList().subscribe().unsubscribe();
    clearInterval(this.interval)
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
        this.history = res.history;
        this.filteredHistory = this.history;
        this.empty = res.empty;
      },
      error => {
        if (error.statusText === "Unknown Error") {
          // show error message if no connexion
          this._commonService.regularToaster(
            "error",
            "Une erreur s'est produite : contactez l'administrateur du site"
          )
        }
        else if (error.status === 404) {
          this._commonService.regularToaster("warning", "Aucun import trouvé");

        } else {
          // show error message if other server error
          this._commonService.regularToaster("error", error.error.message);
        }
      }
    );
  }

  onFinishImport(row) {
    localStorage.setItem("startPorcess", JSON.stringify(true));
    let dataStep1: Step1Data = {
      importId: row.id_import,
      datasetId: row.id_dataset,
      formData: {
        fileName: row.full_file_name,
        encoding: row.encoding,
        srid: row.srid,
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

    if (row.step == 1) {
      this._router.navigate([
        `${ModuleConfig.MODULE_URL}/process/step/${row.step}`
      ]);
    } else {
      this._router.navigate([
        `${ModuleConfig.MODULE_URL}/process/id_import/${row.id_import}/step/${row.step}`
      ]);
    }

  }

  onViewDataset(row) {
    this._router.navigate([
      `metadata/dataset_detail/${row.id_dataset}`
    ]);
  }

  openDeleteModal(row, modalDelete) {
    this.deleteOne = row;
    this.modal.open(modalDelete);
  }

}
