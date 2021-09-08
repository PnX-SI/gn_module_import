import { Component, OnInit, ViewChild, ViewEncapsulation } from "@angular/core";
import { Router } from "@angular/router";
import { DataService } from "../../../services/data.service";
import { FieldMappingService } from "../../../services/mappings/field-mapping.service";
import { CommonService } from "@geonature_common/service/common.service";
import { CruvedStoreService } from "@geonature_common/service/cruved-store.service";

import { ModuleConfig } from "../../../module.config";
import {
  FormControl,
  FormGroup,
  FormBuilder,
  Validators
} from "@angular/forms";
import {
  StepsService,
  Step2Data,
  Step3Data,
  Step4Data
} from "../steps.service";
import { forkJoin } from "rxjs/observable/forkJoin";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";

@Component({
  selector: "fields-mapping-step",
  styleUrls: ["fields-mapping-step.component.scss"],
  templateUrl: "fields-mapping-step.component.html",
  encapsulation: ViewEncapsulation.None

})
export class FieldsMappingStepComponent implements OnInit {
  public spinner: boolean = false;
  public displayAllValues: boolean = ModuleConfig.DISPLAY_MAPPED_VALUES;
  public IMPORT_CONFIG = ModuleConfig;
  public syntheseForm: FormGroup;
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
  public userFieldMappings;
  public test: Array<any>;
  public fieldMappingForm = new FormControl();
  public newMappingForm = new FormControl();
  public mappedColCount: number;
  public unmappedColCount: number;
  public mappedList = [];
  public nbLignes: number;
  @ViewChild("modalConfirm") modalConfirm: any;
  @ViewChild("modalRedir") modalRedir: any;
  @ViewChild("modalCreate") modalCreate: any;
  @ViewChild("modalChange") modalChange: any;
  public createMappingModal: NgbModal;
  public changeMappingModal: NgbModal;
  constructor(
    private _ds: DataService,
    private _fm: FieldMappingService,
    private _commonService: CommonService,
    private _fb: FormBuilder,
    private stepService: StepsService,
    private _router: Router,
    private _modalService: NgbModal,
    public cruvedStore: CruvedStoreService
  ) { }

  ngOnInit() {

    this.stepData = this.stepService.getStepData(2);
    const forkJoinArray = [
      this._ds.getColumnsImport(this.stepData.importId),
      this._ds.getMappings("field"),
      this._ds.getOneImport(this.stepData.importId)
    ]

    if (this.stepData.id_field_mapping) {

      forkJoinArray.push(
        this._ds.getOneBibMapping(this.stepData.id_field_mapping)
      )

    }
    // get all columns of the import and the mapping
    forkJoin(forkJoinArray).subscribe(
      data => {
        this.setColumnsImport(data[0]);
        this.userFieldMappings = data[1];

        let importData: any = data[2];
        if (data.length == 4) {
          let mapping_already_present = false
          this.userFieldMappings.forEach(element => {
            if (element.id_mapping == this.stepData.id_field_mapping) {
              mapping_already_present = true;
            }
          });
          if (!mapping_already_present) {
            this.userFieldMappings.push(data[3])
          }

          if (importData.id_field_mapping) {
            this.fieldMappingForm.setValue(
              this.userFieldMappings.find(mapping => mapping.id_mapping == importData.id_field_mapping)
            );
          }
        }


        // subscribe to mapping change
        this.fieldMappingForm.valueChanges
          .filter(value => value !== null && value !== undefined)
          .subscribe(mapping => {
            this.onMappingChange(mapping.id_mapping);
          });
        if (this.userFieldMappings.length == 1) {
          this.fieldMappingForm.setValue(this.userFieldMappings[0]);
        }
      },
      error => {
        console.error(error);
        if (error.statusText === "Unknown Error") {
          // show error message if no connexion
          this._commonService.regularToaster(
            "error",
            "Une erreur s'est produite : contactez l'administrateur du site"
          );
        } else {
          this._commonService.regularToaster("error", error.error);
        }
      }
    );

    this.syntheseForm = this._fb.group({});
    if (this.stepData.mappingRes) {
      this.mappingRes = this.stepData.mappingRes;
      this.table_name = this.mappingRes["table_name"];
      this.mappingIsValidate = true;
    }
    this.generateSyntheseForm();
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

  canUpdateMapping() {
    if (
      !ModuleConfig.ALLOW_MODIFY_DEFAULT_MAPPING &&
      ModuleConfig.ALLOW_FIELD_MAPPING &&
      this.id_mapping == this.IMPORT_CONFIG.DEFAULT_FIELD_MAPPING_ID
    ) {
      return true;
    }
    return false;
  }

  updateMappingField(value, id_mapping) {
    this.spinner = true;
    this._ds
      .updateMappingField(value, this.stepData.importId, id_mapping)
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

  onRedirect() {
    this._router.navigate([ModuleConfig.MODULE_URL]);
  }

  setMappingInfo(temporary ) {
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
      mappingIsValidate: true,
      temporaryMapping: temporary,
      cruvedMapping: this.fieldMappingForm.value.cruved
    };
    this.stepService.setStepData(2, step2data);

    const mappingData = Object.assign({}, this.syntheseForm.value);
    this._ds
      .createOrUpdateFieldMapping(mappingData, this.id_mapping)
      .subscribe(data => {
        // update t_imports (set information about autogenerate values)
        this.spinner = true;
        const formValue = this.syntheseForm.getRawValue();
        
        const importFormData = {
          uuid_autogenerated:
          formValue.unique_id_sinp_generate || false,
          altitude_autogenerated:
          formValue.altitudes_generate || false,
          id_field_mapping:
            this.fieldMappingForm.value.id_mapping
        };
        this._ds
          .updateImport(this.stepData.importId, importFormData)
          .subscribe(d => {
            console.log("Done");
          });

        if (!ModuleConfig.ALLOW_VALUE_MAPPING) {
          this._ds
            .dataChecker(
              this.stepData.importId,
              this.id_mapping,
              ModuleConfig.DEFAULT_VALUE_MAPPING_ID
            )
            .subscribe(
              import_obj => {
                this.spinner = false;
                let step4Data: Step4Data = {
                  importId: this.stepData.importId
                };
                if (import_obj.source_count < ModuleConfig.MAX_LINE_LIMIT) {
                  this.stepService.setStepData(4, step4Data);
                  this._router.navigate([
                    `${ModuleConfig.MODULE_URL}/process/id_import/${import_obj.id_import}/step/4`
                  ]);
                } else {
                  this.nbLignes = import_obj.source_count;
                  this._modalService.open(this.modalRedir);
                }
              },
              error => {
                this.spinner = false;
                this._commonService.regularToaster(
                  "error",
                  error.error.message
                );
              }
            );
        } else {
          this._router.navigate([`${ModuleConfig.MODULE_URL}/process/id_import/${this.stepData.importId}/step/3`]);
        }
      });
  }

  // On close modal: ask if save the mapping or not
  saveMappingUpdate(saveBoolean) {
    if (saveBoolean) {
      this.setMappingInfo(false);
    } else {
      // create a temporary mapping
      const mapping_value = {
        mappingName: "mapping_temporaire_" + Date.now(),
        temporary: true
      };
      this._ds.postMappingName(mapping_value, "FIELD").subscribe(id_mapping => {
        this.id_mapping = id_mapping;
        this.setMappingInfo(true);
      });
    }
  }

  openModalConfirm() {
    this._modalService.open(this.modalConfirm);
  }

  onNextStep() {
    // if the form has changed
    if (!this.syntheseForm.pristine) {
      // if the user has right to modify it
      if (this.fieldMappingForm.value.cruved.U) {
        this.openModalConfirm();
        // the form has changed but the user do not has rights
      } else {
        const mapping_value = {
          mappingName: "mapping_temporaire_" + Date.now(),
          temporary: true
        };
        this._ds
          .postMappingName(mapping_value, "FIELD")
          .subscribe(id_mapping => {
            this.id_mapping = id_mapping;
            this.setMappingInfo(true);
          });
      }
      // the form not change do not create temp mapping
    } else {
      this.setMappingInfo(false);
    }
  }

  onFormMappingChange() {
    this.syntheseForm.valueChanges.subscribe(() => {
      if (this.mappingIsValidate) {
        this.mappingRes = null;
        this.mappingIsValidate = false;
        this.table_name = null;
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

  compareFn(c1: any, c2: any): boolean {
    return c1 && c2 ? c1.id_mapping === c2.id_mapping : c1 === c2;
  }

  setColumnsImport(columns) {
    this.columns = columns.map(col => {
      return {
        id: col,
        selected: false
      };
    });

    if (this.stepData.id_field_mapping) {
      const formValue = {
        id_mapping: this.stepData.id_field_mapping,
        cruved: this.stepData.cruvedMapping
      };
      this.fieldMappingForm.setValue(formValue);
      this.fillMapping(this.stepData.id_field_mapping, this.columns);
    } else {
      this.formReady = true;
    }
  }

  getMappingNamesList(mapping_type) {
    this._ds.getMappings(mapping_type).subscribe(
      result => {
        this.userFieldMappings = result;
        if (result.length == 1) {
          this.fieldMappingForm.setValue(result[0]);
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
            
            if (field["target_field"] == 'unique_id_sinp_generate') {
              const form = this.syntheseForm
                .get('unique_id_sinp_generate');
                if(form) {
                  form.setValue(field["source_field"] == 'true')
                }
            }
            if (field["target_field"] == 'altitudes_generate') {
              const form = this.syntheseForm
                .get('altitudes_generate')
              if(form) {
                form.setValue(field["source_field"] == 'true');
              }
                
            }

            if (columnsArray.includes(field["source_field"])) {
              const target_form = this.syntheseForm.get(field["target_field"])
              if (target_form) {
                target_form.setValue(field["source_field"]);
                this.mappedList.push(field["target_field"]);
              }

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
    this.newMappingForm.reset();
    this.createMappingModal = this._modalService.open(this.modalCreate);
    this.displayAllValues = true;
  }

  cancelMapping() {
    this.newMappingForm.reset();
  }

  renameMapping() {
    this.newMappingForm.setValue(this.fieldMappingForm.value.mapping_label);
    this.changeMappingModal = this._modalService.open(this.modalChange);
  }

  saveMappingName(mappingName, targetForm) {
    let mappingType = "FIELD";
    const value = {};
    value["mappingName"] = mappingName;
    this._ds.postMappingName(value, mappingType).subscribe(
      new_id_mapping => {
        this.stepData.id_field_mapping = new_id_mapping;
        this.newMappingForm.reset();
        this._ds.getMappings("FIELD").subscribe(result => {
          this.userFieldMappings = result;
          const newMapping = this.userFieldMappings.find(
            el => el.id_mapping == new_id_mapping
          );
          this.fieldMappingForm.setValue(newMapping);
        });

        this.enableMapping(targetForm);
        this.createMappingModal.close()
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

  updateMappingName(value) {
    let mappingType = "FIELD";

    this._ds.updateMappingName(value, mappingType, this.id_mapping).subscribe(
      res => {
        this.stepData.id_field_mapping = res;
        this.getMappingNamesList(mappingType);
        this.changeMappingModal.close()
        // this.fieldMappingForm.controls["fieldMapping"].setValue(res);
        // this.fieldMappingForm.controls["mappingName"].setValue("");
        //this.enableMapping(targetForm);
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
    this.stepData.importId;
  }

  count(targetForm) {
    this.mappedColCount = 0;
    this.unmappedColCount = 0;
    for (const field in targetForm.controls) {
      let colValue = targetForm.get(field).value;
      if (colValue != "WKT" && field != "autogenerated") {
        if (colValue == null || colValue == "") {
          this.unmappedColCount = this.unmappedColCount + 1;
        } else {
          this.mappedColCount = this.mappedColCount + 1;
        }
      }
    }
  }

  checkCondition(name_field) {
    if (this.IMPORT_CONFIG.DISPLAY_CHECK_BOX_MAPPED_FIELD) {
      if (!this.displayAllValues) {
        return !this.mappedList.includes(name_field);
      }
      return true;
    }

    if (this.IMPORT_CONFIG.DISPLAY_MAPPED_FIELD) {
      return !this.mappedList.includes(name_field);
    } else {
      return true;
    }
  }

  onSelect(id_mapping, targetForm) {
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
