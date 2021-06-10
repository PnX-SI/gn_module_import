import { Component, OnInit } from "@angular/core";
import { ActivatedRoute, Router } from "@angular/router";
import { Observable, timer, of } from "rxjs";
import { map, take, concatMap } from 'rxjs/operators';
import { DataService } from "../../../services/data.service";
import { CommonService } from "@geonature_common/service/common.service";
import { ModuleConfig } from "../../../module.config";
import { FormGroup, FormBuilder, Validators, AbstractControl, ValidatorFn } from "@angular/forms";
import { Step } from "../../../models/enums.model";
import { Import } from "../../../models/import.model";
import { ImportProcessComponent } from "../import-process.component";
import { ImportProcessService } from "../import-process.service";

@Component({
  selector: "upload-file-step",
  styleUrls: ["upload-file-step.component.scss"],
  templateUrl: "upload-file-step.component.html"
})
export class UploadFileStepComponent implements OnInit {
  private step: Step;
  private importData: Import;
  private datasetId: number = null;
  private uploadForm: FormGroup;
  private file: File | null = null;
  private fileName: string;
  private isUploadRunning: boolean = false;

  constructor(
    private ds: DataService,
    private commonService: CommonService,
    private fb: FormBuilder,
    private importProcessService: ImportProcessService,
    private router: Router,
    private route: ActivatedRoute,
  ) {
    this.uploadForm = this.fb.group({
      file: [null, Validators.required],
      fileName: [null, [Validators.required, Validators.maxLength(50)]],
    });
  }

  ngOnInit() {
    this.step = this.route.snapshot.data.step;
    this.importData = this.importProcessService.getImportData();
    if (this.importData === null) {
      this.datasetId = this.route.snapshot.queryParams["datasetId"];
    } else {
      this.fileName = this.importData.full_file_name;
    }
  }

  submitAvailable() {
    if (this.isUploadRunning) {
      return false;
    } else if (this.importData && this.uploadForm.pristine) {
      return true;
    } else {
      return this.uploadForm.valid;
    }
  }

  onFileSelected(file: File) {
    this.file = file;
    this.fileName = file.name;
    this.uploadForm.setValue({
        file: this.file,
        fileName: this.fileName,
    });
    this.uploadForm.markAsDirty();
  }

  onNextStep() {
      if (this.uploadForm.pristine) {
          this.importProcessService.navigateToNextStep(this.step);
          return;
      }
      this.isUploadRunning = true;
      var upload: Observable<Import>;
      if (this.importData) {
          upload = this.ds.updateFile(this.importData.id_import, this.file);
      } else {
          upload = this.ds.addFile(this.datasetId, this.file);
      }
      upload.subscribe(
            res => {
              this.isUploadRunning = false;
              this.importProcessService.setImportData(res);
              this.importProcessService.navigateToLastStep();
            },
            error => {
              this.isUploadRunning = false;
              this.commonService.regularToaster("error", error.error.description);
            },
        );
  }
}
