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
  ) { }

  getMappingNamesList(mapping_type) {
    this._ds.getMappings(mapping_type).subscribe(
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
          this.geoFormValidator(targetFormName);
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

  /**
   * Add custom validator to the form
   */
  geoFormValidator(targetForm) {
    /*
        6 cases :
        - all empty : all required
        - wkt == '' and both coordinates != '' : wkt not required, codes not required, coordinates required
        - wkt != '' : wkt required, coordinates and codes not required
        - one of the code not empty: others not required
        */
    if (
      targetForm.get("WKT").value === "" &&
      (targetForm.get("longitude").value === "" ||
        targetForm.get("latitude").value === "")
      && (
        targetForm.get("codemaille").value === "" ||
        targetForm.get("codecommune").value === "" ||
        targetForm.get("codedepartement").value === ""
      )
    ) {
      this.setFormControlRequired(targetForm, "WKT");
      this.setFormControlRequired(targetForm, "longitude");
      this.setFormControlRequired(targetForm, "latitude");
      this.setFormControlRequired(targetForm, "codemaille");
      this.setFormControlRequired(targetForm, "codecommune");
      this.setFormControlRequired(targetForm, "codedepartement");
    }
    if (
      targetForm.get("WKT").value === "" &&
      targetForm.get("longitude").value !== "" &&
      targetForm.get("latitude").value !== ""
    ) {
      this.setFormControlNotRequired(targetForm, "WKT");
      this.setFormControlRequired(targetForm, "longitude");
      this.setFormControlRequired(targetForm, "latitude");
      this.setFormControlNotRequired(targetForm, "codecommune");
      this.setFormControlNotRequired(targetForm, "codedepartement");
      this.setFormControlNotRequired(targetForm, "codemaille");
    }
    if (targetForm.get("WKT").value !== "") {
      this.setFormControlRequired(targetForm, "WKT");
      this.setFormControlNotRequired(targetForm, "longitude");
      this.setFormControlNotRequired(targetForm, "latitude");
      this.setFormControlNotRequired(targetForm, "codemaille");
      this.setFormControlNotRequired(targetForm, "codecommune");
      this.setFormControlNotRequired(targetForm, "codedepartement");
    }
    if (targetForm.get("codemaille").value !== "") {
      this.setFormControlRequired(targetForm, "codemaille");
      this.setFormControlNotRequired(targetForm, "WKT");
      this.setFormControlNotRequired(targetForm, "longitude");
      this.setFormControlNotRequired(targetForm, "latitude");
      this.setFormControlNotRequired(targetForm, "codecommune");
      this.setFormControlNotRequired(targetForm, "codedepartement");
    }
    if (targetForm.get("codecommune").value !== "") {
      this.setFormControlRequired(targetForm, "codecommune");
      this.setFormControlNotRequired(targetForm, "WKT");
      this.setFormControlNotRequired(targetForm, "longitude");
      this.setFormControlNotRequired(targetForm, "latitude");
      this.setFormControlNotRequired(targetForm, "codemaille");
      this.setFormControlNotRequired(targetForm, "codedepartement");
    }
    if (targetForm.get("codedepartement").value !== "") {
      this.setFormControlRequired(targetForm, "codedepartement");
      this.setFormControlNotRequired(targetForm, "WKT");
      this.setFormControlNotRequired(targetForm, "longitude");
      this.setFormControlNotRequired(targetForm, "latitude");
      this.setFormControlNotRequired(targetForm, "codecommune");
      this.setFormControlNotRequired(targetForm, "codemaille");
    }
  }

  onSelect(id_mapping, targetForm) {
    this.id_mapping = id_mapping;
    this.shadeSelectedColumns(targetForm);
    this.geoFormValidator(targetForm);
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
