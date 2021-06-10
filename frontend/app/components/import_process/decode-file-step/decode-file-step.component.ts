import { Component, OnInit, Input } from "@angular/core";
import { ActivatedRoute, Router } from "@angular/router";
import { DataService } from "../../../services/data.service";
import { CommonService } from "@geonature_common/service/common.service";
import { ModuleConfig } from "../../../module.config";
import { FormGroup, FormBuilder, Validators, AbstractControl, ValidatorFn } from "@angular/forms";
import { ImportProcessComponent } from "../import-process.component";
import { ImportProcessService } from "../import-process.service";
import { Step } from "../../../models/enums.model";
import { Import } from "../../../models/import.model";

@Component({
  selector: "decode-file-step",
  styleUrls: ["decode-file-step.component.scss"],
  templateUrl: "decode-file-step.component.html"
})
export class DecodeFileStepComponent implements OnInit {
  private step: Step;
  private importData: Import;
  private paramsForm: FormGroup;
  private importConfig = ModuleConfig;
  private isRequestPending: boolean = false; // spinner

  constructor(
    private fb: FormBuilder,
    private ds: DataService,
    private importProcessService: ImportProcessService,
    private commonService: CommonService,
    private router: Router,
    private route: ActivatedRoute,
  ) {
    this.paramsForm = this.fb.group({
      encoding: [null, Validators.required],
      format: [null, Validators.required],
      srid: [null, Validators.required],
    });
  }

  ngOnInit() {
    this.step = this.route.snapshot.data.step;
    this.importData = this.importProcessService.getImportData();
    if (this.importData.encoding) {
      this.paramsForm.patchValue({ encoding: this.importData.encoding });
    } else if (this.importData.detected_encoding) {
      this.paramsForm.patchValue({ encoding: this.importData.detected_encoding });
    }
    if (this.importData.format_source_file) {
      this.paramsForm.patchValue({ format: this.importData.format_source_file });
    } else if (this.importData.detected_format) {
      this.paramsForm.patchValue({ format: this.importData.detected_format });
    }
    if (this.importData.srid) {
      this.paramsForm.patchValue({ srid: this.importData.srid });
    }
  }

  onPreviousStep() {
    this.importProcessService.navigateToPreviousStep(this.step);
  }

  submitAvailable() {
    return this.paramsForm.valid;
  }

  onSubmit() {
    if (this.paramsForm.pristine && this.importData.step > Step.Decode) {
        this.importProcessService.navigateToNextStep(this.step);
        return;
    }
    this.isRequestPending = true;
    this.ds.decodeFile(
        this.importData.id_import,
        this.paramsForm.value,
      ).subscribe(
        res => {
            this.isRequestPending = false;
            this.importProcessService.setImportData(res);
            this.importProcessService.navigateToLastStep();
        },
        error => {
          this.isRequestPending = false;
          this.commonService.regularToaster("error", error.error.description);
        },
      );
  }
}
