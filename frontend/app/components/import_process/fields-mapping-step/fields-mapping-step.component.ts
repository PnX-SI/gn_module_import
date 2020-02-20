import { Component, OnInit } from "@angular/core";
import { Router } from "@angular/router";
import { DataService } from "../../../services/data.service";
import { FieldMappingService } from "../../../services/mappings/field-mapping.service";
import { CommonService } from "@geonature_common/service/common.service";
import { ModuleConfig } from "../../../module.config";
import {
  FormControl,
  FormGroup,
  FormBuilder,
  Validators
} from "@angular/forms";
import { StepsService, Step2Data, Step3Data } from "../steps.service";
import { forkJoin } from "rxjs/observable/forkJoin";

@Component({
  selector: "fields-mapping-step",
  styleUrls: ["fields-mapping-step.component.scss"],
  templateUrl: "fields-mapping-step.component.html"
})
export class FieldsMappingStepComponent implements OnInit {
  public spinner: boolean = false;
  public IMPORT_CONFIG = ModuleConfig;
  public syntheseForm: FormGroup;
  public n_error_lines;
  public isErrorButtonClicked: boolean = false;
  public dataCleaningErrors;
  public isFullError: boolean = true;
  public id_mapping;
  public step3Response;
  public table_name;
  mappingIsValidate: boolean = false;
  isUserError: boolean = false;
  formReady: boolean = false;
  columns: any;
  mappingRes: any;
  bibRes: any;
  stepData: Step2Data;
  public fieldMappingForm: FormGroup;
  public userFieldMappings;
  public newMapping: boolean = false;

  constructor(
    private _ds: DataService,
    private _fm: FieldMappingService,
    private _commonService: CommonService,
    private _fb: FormBuilder,
    private stepService: StepsService,
    private _router: Router
  ) {}

  ngOnInit() {
    this.stepData = this.stepService.getStepData(2);
    this.fieldMappingForm = this._fb.group({
      fieldMapping: [null],
      mappingName: [""]
    });
    this.syntheseForm = this._fb.group({});

    if (this.stepData.mappingRes) {
      this.mappingRes = this.stepData.mappingRes;
      this.table_name = this.mappingRes["table_name"];
      this.getValidationErrors(
        this.mappingRes["n_table_rows"],
        this.stepData.importId
      );
      this.mappingIsValidate = true;
    }
    this.generateSyntheseForm();
  }

  getValidationErrors(n_table_rows, importId) {
    let getErrorList = this._ds.getErrorList(importId);
    let getRowErrorCount = this._ds.checkInvalid(importId);
    forkJoin([getErrorList, getRowErrorCount]).subscribe(
      ([errorList, errorCount]) => {
        this.dataCleaningErrors = errorList;
        this.n_error_lines = Number(errorCount);
        this.isFullErrorCheck(n_table_rows, this.n_error_lines);
      },
      error => {
        if (error.statusText === "Unknown Error") {
          // show error message if no connexion
          this._commonService.regularToaster(
            "error",
            "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
          );
        } else {
          // show error message if other server error
          console.error(error);
          this._commonService.regularToaster("error", error.error.message);
        }
      }
    );
  }

  generateSyntheseForm() {
    this._ds.getBibFields().subscribe(
      res => {
        this.bibRes = res;
        const validators = [Validators.required];
        for (let theme of this.bibRes) {
          for (let field of theme.fields) {
            if (field.required) {
              this.syntheseForm.addControl(
                field.name_field,
                new FormControl({ value: "", disabled: true }, validators)
              );
              this.syntheseForm
                .get(field.name_field)
                .setValidators([Validators.required]);
            } else {
              this.syntheseForm.addControl(
                field.name_field,
                new FormControl({ value: "", disabled: true })
              );
            }
          }
        }
        this.getMappingNamesList("field", this.stepData.importId);
        this.geoTypeSelect(this.syntheseForm);
      },
      error => {
        if (error.statusText === "Unknown Error") {
          // show error message if no connexion
          this._commonService.regularToaster(
            "error",
            "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
          );
        } else {
          // show error message if other server error
          console.error(error);
          this._commonService.regularToaster("error", error.error.message);
        }
      }
    );
  }

  onDataCleaning(value, id_mapping) {
    this.spinner = true;
    this._ds
      .postMapping(
        value,
        this.stepData.importId,
        id_mapping,
        this.stepData.srid
      )
      .subscribe(
        res => {
          this.mappingRes = res;
          this.spinner = false;
          this.getValidationErrors(res["n_table_rows"], this.stepData.importId);
          this.table_name = this.mappingRes["table_name"];
          this.mappingIsValidate = true;
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
            if (error.status == 400) this.isUserError = true;
            this._commonService.regularToaster("error", error.error.message);
          }
        }
      );
  }

  onNextStep() {
    if (this.mappingRes == undefined) {
      this._commonService.regularToaster(
        "error",
        "Veuillez valider le mapping avant de passer à l'étape suivante"
      );
    }
    this._ds
      .postMetaToStep3(
        this.stepData.importId,
        this.id_mapping,
        this.mappingRes["table_name"]
      )
      .subscribe(
        res => {
          this.step3Response = res;
          let step3data: Step3Data = {
            table_name: this.step3Response.table_name,
            importId: this.stepData.importId
          };
          let savedStep3: Step3Data = this.stepService.getStepData(3);
          if (savedStep3 && savedStep3.id_content_mapping)
            step3data.id_content_mapping = savedStep3.id_content_mapping;
          let step2data: Step2Data = {
            importId: this.stepData.importId,
            srid: this.stepData.srid,
            id_field_mapping: this.id_mapping,
            mappingRes: this.mappingRes,
            mappingIsValidate: this.mappingIsValidate
          };

          this.stepService.setStepData(3, step3data);
          this.stepService.setStepData(2, step2data);
          this._router.navigate([`${ModuleConfig.MODULE_URL}/process/step/3`]);
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
            console.error(error);
            this._commonService.regularToaster("error", error.error.message);
          }
        }
      );
  }

  onFormMappingChange() {
    this.syntheseForm.valueChanges.subscribe(() => {
      if (this.mappingIsValidate) {
        this.mappingRes = null;
        this.mappingIsValidate = false;
        this.table_name = null;
        this.n_error_lines = null;
        let step2data = this.stepData;
        step2data.mappingIsValidate = false;
        this.stepService.setStepData(2, step2data);
      }
    });
  }

  onStepBack() {
    this._router.navigate([`${ModuleConfig.MODULE_URL}/process/step/1`]);
  }

  ErrorButtonClicked() {
    this.isErrorButtonClicked = !this.isErrorButtonClicked;
  }

  isFullErrorCheck(n_table_rows, n_errors) {
    if (n_table_rows == n_errors) {
      this.isFullError = true;
      this._commonService.regularToaster(
        "warning",
        "Attention, toutes les lignes ont des erreurs"
      );
    } else {
      this.isFullError = false;
    }
  }

  getMappingNamesList(mapping_type, importId) {
    this._ds.getMappings(mapping_type, importId).subscribe(
      result => {
        this.userFieldMappings = result["mappings"];
        if (result["column_names"] != "undefined import_id") {
          this.columns = result["column_names"].map(col => {
            return {
              id: col,
              selected: false
            };
          });
        }

        if (this.stepData.id_field_mapping) {
          this.fieldMappingForm.controls["fieldMapping"].setValue(
            this.stepData.id_field_mapping
          );
          this.fillMapping(this.stepData.id_field_mapping, this.syntheseForm);
        } else {
          this.formReady = true;
          this.onMappingName(this.fieldMappingForm, this.syntheseForm);
        }
      },
      error => {
        console.error(error);
        if (error.statusText === "Unknown Error") {
          // show error message if no connexion
          this._commonService.regularToaster(
            "error",
            "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
          );
        } else {
          this._commonService.regularToaster("error", error.error);
        }
      }
    );
  }

  onMappingName(mappingForm, targetFormName): void {
    mappingForm.get("fieldMapping").valueChanges.subscribe(
      id_mapping => {
        this.id_mapping = id_mapping;
        if (this.id_mapping && id_mapping != "") {
          this.fillMapping(this.id_mapping, targetFormName);
        } else {
          this.fillEmptyMapping(targetFormName);
          this.disableMapping(targetFormName);
          this.shadeSelectedColumns(targetFormName);
        }
      },
      error => {
        if (error.statusText === "Unknown Error") {
          // show error message if no connexion
          this._commonService.regularToaster(
            "error",
            "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
          );
        } else {
          console.error(error);
          this._commonService.regularToaster("error", error.error);
        }
      }
    );
  }

  fillMapping(id_mapping, targetFormName) {
    this.id_mapping = id_mapping;
    this._ds.getMappingFields(this.id_mapping).subscribe(
      mappingFields => {
        if (mappingFields[0] != "empty") {
          for (let field of mappingFields) {
            this.enableMapping(targetFormName);
            targetFormName
              .get(field["target_field"])
              .setValue(field["source_field"]);
          }
          this.shadeSelectedColumns(targetFormName);
          this.geoTypeSelect(targetFormName);
        } else {
          this.fillEmptyMapping(targetFormName);
        }
        this.onFormMappingChange();
        this.onMappingName(this.fieldMappingForm, this.syntheseForm);
        this.formReady = true;
      },
      error => {
        if (error.statusText === "Unknown Error") {
          // show error message if no connexion
          this._commonService.regularToaster(
            "error",
            "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
          );
        } else {
          console.error(error);
          this._commonService.regularToaster("error", error.error);
        }
      }
    );
  }

  createMapping() {
    this.fieldMappingForm.reset();
    this.newMapping = true;
  }

  cancelMapping() {
    this.newMapping = false;
    this.fieldMappingForm.controls["mappingName"].setValue("");
  }

  saveMappingName(value, importId, targetForm) {
    let mappingType = "FIELD";
    this._ds.postMappingName(value, mappingType).subscribe(
      res => {
        this.stepData.id_field_mapping = res;
        this.newMapping = false;
        this.getMappingNamesList(mappingType, importId);
        this.fieldMappingForm.controls["fieldMapping"].setValue(res);
        this.fieldMappingForm.controls["mappingName"].setValue("");
        this.enableMapping(targetForm);
      },
      error => {
        if (error.statusText === "Unknown Error") {
          // show error message if no connexion
          this._commonService.regularToaster(
            "error",
            "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
          );
        } else {
          console.error(error);
          this._commonService.regularToaster("error", error.error);
        }
      }
    );
  }

  shadeSelectedColumns(targetFormName) {
    let formValues = targetFormName.value;
    this.columns.map(col => {
      if (formValues) {
        if (Object.values(formValues).includes(col.id)) {
          col.selected = true;
        } else {
          col.selected = false;
        }
      }
    });
  }

  setFormControlNotRequired(targetForm, formControlName) {
    targetForm.get(formControlName).clearValidators();
    targetForm.get(formControlName).setValidators(null);
    targetForm.get(formControlName).updateValueAndValidity();
  }

  setFormControlRequired(targetForm, formControlName) {
    targetForm.get(formControlName).setValidators([Validators.required]);
    targetForm.get(formControlName).updateValueAndValidity();
  }

  geoTypeSelect(targetForm) {
    /*
        3 cases :
        - one coordinates == '' && wkt == '' : lat && long && wkt set as required
        - wkt == '' and both coordinates != '' : wkt not required, coordinates required
        - wkt != '' : wkt required, coordinates not required
        */
    if (
      targetForm.get("WKT").value === "" &&
      (targetForm.get("longitude").value === "" ||
        targetForm.get("latitude").value === "")
    ) {
      this.setFormControlRequired(targetForm, "WKT");
      this.setFormControlRequired(targetForm, "longitude");
      this.setFormControlRequired(targetForm, "latitude");
    }
    if (
      targetForm.get("WKT").value === "" &&
      targetForm.get("longitude").value !== "" &&
      targetForm.get("latitude").value !== ""
    ) {
      this.setFormControlNotRequired(targetForm, "WKT");
      this.setFormControlRequired(targetForm, "longitude");
      this.setFormControlRequired(targetForm, "latitude");
    }
    if (targetForm.get("WKT").value !== "") {
      this.setFormControlRequired(targetForm, "WKT");
      this.setFormControlNotRequired(targetForm, "longitude");
      this.setFormControlNotRequired(targetForm, "latitude");
    }
  }

  onSelect(id_mapping, targetForm) {
    this.id_mapping = id_mapping;
    this.shadeSelectedColumns(targetForm);
    this.geoTypeSelect(targetForm);
  }

  disableMapping(targetForm) {
    Object.keys(targetForm.controls).forEach(key => {
      targetForm.controls[key].disable();
    });
  }

  enableMapping(targetForm) {
    Object.keys(targetForm.controls).forEach(key => {
      targetForm.controls[key].enable();
    });
  }

  fillEmptyMapping(targetForm) {
    Object.keys(targetForm.controls).forEach(key => {
      targetForm.get(key).setValue("");
    });
  }
}
