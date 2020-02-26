import { Injectable } from "@angular/core";
import { FormGroup, Validators } from "@angular/forms";
import { DataService } from "../data.service";
import { CommonService } from "@geonature_common/service/common.service";

@Injectable()
export class FieldMappingService {
  public fieldMappingForm: FormGroup;
  public userFieldMappings;
  public columns;
  public newMapping: boolean = false;
  public id_mapping;

  constructor(
    private _ds: DataService,
    private _commonService: CommonService
  ) {}

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
