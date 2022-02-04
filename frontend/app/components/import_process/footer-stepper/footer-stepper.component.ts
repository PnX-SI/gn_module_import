import { Component, OnInit, Input } from "@angular/core";
import { Router, ActivatedRoute } from "@angular/router";
import { DataService } from "../../../services/data.service";
import { CommonService } from "@geonature_common/service/common.service";
import { ModuleConfig } from "../../../module.config";
import { ImportProcessService } from "../import-process.service";
import { ImportStepInterface } from "../import-process.interface";
import { isObservable } from "rxjs";
import { Import } from "../../../models/import.model";


@Component({
  selector: "footer-stepper",
  styleUrls: ["footer-stepper.component.scss"],
  templateUrl: "footer-stepper.component.html"
})
export class FooterStepperComponent implements OnInit {
  @Input() stepComponent;
  public IMPORT_CONFIG = ModuleConfig;

  constructor(
    private _router: Router,
    private _route: ActivatedRoute,
    private _ds: DataService,
    private importProcessService: ImportProcessService,
    private _commonService: CommonService
  ) { }

  ngOnInit() { }

  deleteImport() {
    let importData: Import | null = this.importProcessService.getImportData();
    if (importData) {
      this._ds.deleteImport(importData.id_import).subscribe(
        () => { this.leaveImport(); }
      );
    } else {
      this.leaveImport();
    }
  }

  saveAndLeaveImport() {
    if (this.stepComponent.onSaveData !== undefined) {
      let ret = this.stepComponent.onSaveData();
      if (isObservable(ret)) {
        ret.subscribe(() => {
          this.leaveImport();
        });
      } else {
        this.leaveImport();
      }
    } else {
      this.leaveImport();
    }
  }

  leaveImport() {
    this.importProcessService.resetImportData();
    this._router.navigate([`${this.IMPORT_CONFIG.MODULE_URL}`]);
  }
}
