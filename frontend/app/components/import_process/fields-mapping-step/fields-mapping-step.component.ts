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
import { StepsService, Step2Data, Step3Data, Step4Data } from "../steps.service";
import { forkJoin } from "rxjs/observable/forkJoin";

@Component({
  selector: "fields-mapping-step",
  styleUrls: ["fields-mapping-step.component.scss"],
  templateUrl: "fields-mapping-step.component.html"
})
export class FieldsMappingStepComponent implements OnInit {
  public spinner: boolean = false;
  public displayAllValues: boolean = false;
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
  public updateMapping: boolean = false; 

  public mappedColCount: number;
  public unmappedColCount: number;
  public mappedList= [];
  constructor(
    private _ds: DataService,
    private _fm: FieldMappingService,
    private _commonService: CommonService,
    private _fb: FormBuilder,
    private stepService: StepsService,
    private _router: Router
  ) { }

  ngOnInit() {
    this.stepData = this.stepService.getStepData(2);
    if (this.IMPORT_CONFIG.ALLOW_FIELD_MAPPING && !this.stepData.id_field_mapping) {      
      this.stepData.id_field_mapping = this.IMPORT_CONFIG.DEFAULT_FIELD_MAPPING_ID;
    }
      
    this.fieldMappingForm = this._fb.group({
      fieldMapping: [null],
      mappingName: [""]
    });
    this.syntheseForm = this._fb.group({});

    // subscribe to mapping change

    this.fieldMappingForm.get("fieldMapping").valueChanges.subscribe(
      id_mapping => {
        this.onMappingChange(id_mapping);
      },
    );

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
        this.dataCleaningErrors = errorList.errors;
        this.n_error_lines = errorList.errors.length;
        this.isFullErrorCheck(n_table_rows, this.n_error_lines);
      },
      error => {
        if (error.status === 500) {
          // show error message if no connexion
          this._commonService.regularToaster(
            "error",
            "ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)"
          );
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
        this._fm.geoFormValidator(this.syntheseForm);
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

  canUpdateMapping()
  {
    console.log(this.id_mapping);
    
    if(!ModuleConfig.ALLOW_MODIFY_DEFAULT_MAPPING &&
         ModuleConfig.ALLOW_FIELD_MAPPING && 
         this.id_mapping == this.IMPORT_CONFIG.DEFAULT_FIELD_MAPPING_ID){
      return true;
    }
    return false;
    
  }

  updateMappingField(value, id_mapping) {
    this.spinner = true;
    this._ds
      .updateMappingField(
        value,
        this.stepData.importId,
        id_mapping,
      )
      .subscribe(
        res => {
          this.spinner = false;
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
    this._ds.updateFieldMapping(this.id_mapping, this.syntheseForm.value).subscribe(data => {
      // update t_imports (set information about autogenerate values)
      this.spinner = true;
      const importFormData = {
        'uuid_autogenerated': this.syntheseForm.value.unique_id_sinp_generate || false,
        'altitude_autogenerated': this.syntheseForm.value.altitudes_generate || false
      }
      this._ds.updateImport(this.stepData.importId, importFormData).subscribe(d => {
        console.log("Done");

      })
      let step3data: Step3Data = {
        //table_name: this.step3Response.table_name,
        importId: this.stepData.importId
      };
      this.stepService.setStepData(3, step3data);
      let step2data: Step2Data = {
        importId: this.stepData.importId,
        srid: this.stepData.srid,
        id_field_mapping: this.id_mapping,
        mappingRes: this.mappingRes,
        mappingIsValidate: true
      };



      this.stepService.setStepData(2, step2data);

      if (!ModuleConfig.ALLOW_VALUE_MAPPING) {
        // this.stepService.setStepData(4, step4Data);
        this._ds.dataChecker(this.stepData.importId, this.id_mapping, ModuleConfig.DEFAULT_MAPPING_ID).subscribe(d => {
          this.spinner = false;
          let step4Data: Step4Data = {
            importId: this.stepData.importId
          };
          this.stepService.setStepData(4, step4Data);
          this._router.navigate([`${ModuleConfig.MODULE_URL}/process/step/4`]);

        },
          error => {
            this.spinner = false;
            this._commonService.regularToaster("error", error.error.message);

          }
        )

      } else {
        this._router.navigate([`${ModuleConfig.MODULE_URL}/process/step/3`]);
      }
    })
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
          this.fillMapping(this.stepData.id_field_mapping, this.columns);
        } else {
          this.formReady = true;
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

  onMappingChange(id_mapping): void {
    console.log(id_mapping)
    this.id_mapping = id_mapping;
    if (this.id_mapping && id_mapping != "") {
      this.fillMapping(this.id_mapping, this.columns);
    } else {
      this.fillEmptyMapping(this.syntheseForm);
      this.disableMapping(this.syntheseForm);
      this.shadeSelectedColumns(this.syntheseForm);
    }
  }

  /**
   * Fill the field form with the value define in the given mapping
   * @param id_mapping : id of the mapping
   * @param fileColumns : columns of the provided file at step 1
   */
  fillMapping(id_mapping, fileColumns) {
    this.id_mapping = id_mapping;
    // build an array from array of object
    const columnsArray: Array<string> = this.columns.map(col => col.id);
    this._ds.getMappingFields(this.id_mapping).subscribe(
      mappingFields => {
        this.mappedList = [];
        this.enableMapping(this.syntheseForm);
        if (mappingFields[0] != "empty") {
        
          for (let field of mappingFields) {
            if (columnsArray.includes(field['source_field'])) {
              this.syntheseForm
                .get(field["target_field"])
                .setValue(field["source_field"]);
                this.mappedList.push(field["target_field"])
                console.log("IF------>"+field["target_field"])
            }
          }
          this.shadeSelectedColumns(this.syntheseForm);
          this._fm.geoFormValidator(this.syntheseForm);
        } else {
          this.fillEmptyMapping(this.syntheseForm);
        }
        this.onFormMappingChange();
        this.count(this.syntheseForm);
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
    this.updateMapping = false;
    this.fieldMappingForm.controls["mappingName"].setValue("");
  }

  renameMapping()
  {
    this.updateMapping = true;
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

  updateMappingName(value, importId, targetForm) {
    let mappingType = "FIELD";
    this._ds.updateMappingName(value, mappingType, this.id_mapping).subscribe(
      res => {
        this.stepData.id_field_mapping = res;
        this.updateMapping = false;
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

  openErrorDetail() {
    this.stepData.importId
  }

  count(targetForm) {
    this.mappedColCount = 0;
    this.unmappedColCount = 0;
    for (const field in targetForm.controls) { 
      let colValue = targetForm.get(field).value;
      if(colValue !="WKT" && field != "autogenerated"){
        if( colValue == null || colValue == ''){
          this.unmappedColCount = this.unmappedColCount + 1
        }else{
          this.mappedColCount = this.mappedColCount + 1;
        }
      }
  }
  console.log(this.unmappedColCount)

  }

  checkCondition(name_field){
    if(this.IMPORT_CONFIG.DISPLAY_CHECK_BOX_MAPPED_FIELD){
      if(!this.displayAllValues){
        return !this.mappedList.includes(name_field);
      }
      return true;
    }

    if(this.IMPORT_CONFIG.DISPLAY_MAPPED_FIELD)
    {
      return !this.mappedList.includes(name_field);
    }else{
      return true;
    }
  }


  onSelect(id_mapping, targetForm) {
    // console.log(targetForm.value)
    this.count(targetForm);
    this.id_mapping = id_mapping;
    this.shadeSelectedColumns(targetForm);
    this._fm.geoFormValidator(targetForm);
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
