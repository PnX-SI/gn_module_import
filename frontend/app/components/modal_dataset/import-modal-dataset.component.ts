import { Component, OnInit, OnDestroy } from "@angular/core";
import { NgbModal, NgbModalRef } from "@ng-bootstrap/ng-bootstrap";
import { FormGroup, FormBuilder, Validators } from "@angular/forms";
import { CommonService } from "@geonature_common/service/common.service";
import { Router } from "@angular/router";
import { DataService } from "../../services/data.service";
import { ModuleConfig } from "../../module.config";
import { StepsService } from "../import_process/steps.service";

@Component({
  selector: "import-modal-dataset",
  templateUrl: "import-modal-dataset.component.html",
  styleUrls: ["./import-modal-dataset.component.scss"]
})
export class ImportModalDatasetComponent implements OnInit, OnDestroy {
  public selectDatasetForm: FormGroup;
  public userDatasetsResponse: any;
  public datasetResponse: JSON;
  public isUserDatasetError: Boolean = false; // true if user does not have any declared dataset
  public datasetError: string;
  private modalRef: NgbModalRef;

  constructor(
    private modalService: NgbModal,
    private _fb: FormBuilder,
    public _ds: DataService,
    private _commonService: CommonService,
    private stepService: StepsService,
    private _router: Router //private _idImport: importIdStorage
  ) {
    this.selectDatasetForm = this._fb.group({
      dataset: ["", Validators.required]
    });
  }

  ngOnInit() {
    this.getUserDataset();
  }

  onOpenModal(content) {
    this.stepService.resetStepoer();
    this.modalRef = this.modalService.open(content, {
      size: "lg"
    });
  }

  closeModal() {
    if (this.modalRef) this.modalRef.close();
  }

  onSubmit() {
    this.stepService.setStepData(1);
    this._router.navigate([`${ModuleConfig.MODULE_URL}/process/step/1`], {
      queryParams: { datasetId: this.selectDatasetForm.value.dataset }
    });
    this.closeModal();
  }

  getUserDataset() {
    // get list of all declared dataset of the user
    this._ds.getUserDatasets().subscribe(
      result => {
        this.userDatasetsResponse = result;
      },
      err => {
        if (err.statusText === "Unknown Error") {
          // show error message if no connexion
          this._commonService.regularToaster(
            "error",
            "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
          );
        } else {
          // show error message if user does not have any declared dataset
          if (err.status == 400) {
            this.isUserDatasetError = true;
            this.datasetError = err.error;
          }
          this._commonService.regularToaster("error", err.error.message);
        }
      }
    );
  }

  ngOnDestroy(): void {
    this.closeModal();
  }
}
