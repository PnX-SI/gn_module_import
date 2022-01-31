import { Component, OnInit, ViewChild, ViewEncapsulation } from "@angular/core";
import { Router, ActivatedRoute } from "@angular/router";
import {
  FormControl,
  FormGroup,
  FormBuilder,
  Validators,
  ValidatorFn,
} from "@angular/forms";
import { HttpErrorResponse } from "@angular/common/http";
import { Observable, of } from "rxjs";
import { forkJoin } from "rxjs/observable/forkJoin";
import { startWith, pairwise, concatMap, map, mapTo, finalize, catchError } from "rxjs/operators";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";

import { CommonService } from "@geonature_common/service/common.service";
import { CruvedStoreService } from "@geonature_common/service/cruved-store.service";

import { DataService } from "../../../services/data.service";
import { FieldMappingService } from "../../../services/mappings/field-mapping.service";
import { ModuleConfig } from "../../../module.config";
import { Import, SynthesisThemeFields } from "../../../models/import.model";
import { ImportProcessComponent } from "../import-process.component";
import { ImportProcessService } from "../import-process.service";
import { Mapping, MappingField } from "../../../models/mapping.model";
import { Step } from "../../../models/enums.model";

@Component({
  selector: "fields-mapping-step",
  styleUrls: ["fields-mapping-step.component.scss"],
  templateUrl: "fields-mapping-step.component.html",
  encapsulation: ViewEncapsulation.None

})
export class FieldsMappingStepComponent implements OnInit {
  public step: Step;
  public importData: Import; // the current import
  public spinner: boolean = false;
  public userFieldMappings: Array<Mapping>; // all field mapping accessible by the users

  public targetFields: Array<SynthesisThemeFields>; // list of target fields, i.e. fields of synthesis, ordered by theme
  public mappedTargetFields: Set<string>;
  public unmappedTargetFields: Set<string>;

  public sourceFields: Array<string>; // list of all source fields of the import
  public mappedSourceFields: Set<string>;
  public unmappedSourceFields: Set<string>;

  public formReady: boolean = false; // do not show frontend fields until all forms are ready
  public fieldMappingForm = new FormControl(); // from to select the mapping to use
  public syntheseForm: FormGroup; // form group to associate each source fields to synthesis fields
  //public displayAllValues: boolean = ModuleConfig.DISPLAY_MAPPED_VALUES; // checkbox to (not) show fields associated by the selected mapping
  public IMPORT_CONFIG = ModuleConfig; // the config of this module
  public canCreateMapping: boolean;
  public canRenameMapping: boolean;
  public canDeleteMapping: boolean;
  public createMappingFormVisible: boolean = false; // show mapping creation form
  public renameMappingFormVisible: boolean = false; // show rename mapping form
  public createOrRenameMappingForm = new FormControl(null, [Validators.required]); // form to add a new mapping
  public modalCreateMappingForm = new FormControl('');
  @ViewChild('saveMappingModal') saveMappingModal;

  constructor(
    private _ds: DataService,
    private _fm: FieldMappingService,
    private _commonService: CommonService,
    private _fb: FormBuilder,
    private importProcessService: ImportProcessService,
    private _router: Router,
    private _route: ActivatedRoute,
    private _modalService: NgbModal,
    public cruvedStore: CruvedStoreService
  ) { }

  ngOnInit() {
    this.step = this._route.snapshot.data.step;
    this.importData = this.importProcessService.getImportData();
    this.canCreateMapping = this.cruvedStore.cruved.IMPORT.module_objects.MAPPING.cruved.C > 0;
    this.canRenameMapping = this.cruvedStore.cruved.IMPORT.module_objects.MAPPING.cruved.U > 0;
    this.canDeleteMapping = this.cruvedStore.cruved.IMPORT.module_objects.MAPPING.cruved.D > 0;
    forkJoin({
      fieldMappings: this._ds.getMappings("field"),
      targetFields: this._ds.getBibFields(),
      sourceFields: this._ds.getColumnsImport(this.importData.id_import),
    }).subscribe(({fieldMappings, targetFields, sourceFields}) => {
      this.userFieldMappings = fieldMappings;

      this.targetFields = targetFields;
      this.mappedTargetFields = new Set();
      this.unmappedTargetFields = new Set();
      this.syntheseForm = this._fb.group({}); //, { validator: this._fm.geoFormValidator });
      this.populateSyntheseForm();
      this.syntheseForm.setValidators([this._fm.geoFormValidator]);
      this.syntheseForm.updateValueAndValidity();

      this.sourceFields = sourceFields;
      this.mappedSourceFields = new Set();
      this.unmappedSourceFields = new Set(sourceFields);

      // subscribe to changes of selected field mapping
      this.fieldMappingForm.valueChanges.subscribe(mapping => {
          this.onNewMappingSelected(mapping);
        });

      // Activate the import field mapping, or the first mapping if import field mapping is not set yet.
      if (this.importData.id_field_mapping) {
        let fieldMapping = this.userFieldMappings.find(mapping => mapping.id_mapping == this.importData.id_field_mapping);
        if (fieldMapping) {
          this.fieldMappingForm.setValue(fieldMapping);
        } else {
          console.log("id field mapping does not exist anymore");
        }
      } else if (this.userFieldMappings.length == 1) {
        this.fieldMappingForm.setValue(this.userFieldMappings[0]);
      }

      this.formReady = true;
    },
    error => {
      console.error(error);
      this._commonService.regularToaster("error", error.error.description);
    });
  }

  // Used by select component to compare field mappings
  areMappingFieldEqual(mf1: MappingField, mf2: MappingField): boolean {
    return mf1 != null && mf2 != null && mf1.id_mapping === mf2.id_mapping;
  }

  // add a form control for each target field in the syntheseForm
  // mandatory target fields have a required validator
  populateSyntheseForm() {
    let validators: Array<ValidatorFn>;
    for (let theme of this.targetFields) {
      for (let field of theme.fields) {
        if (!field.autogenerated) {  // autogenerated = checkbox
          this.unmappedTargetFields.add(field.name_field);
        }
        if (field.mandatory) {
          validators = [Validators.required];
        } else {
          validators = [];
        }
        let control = new FormControl({}, validators);
        control.valueChanges
            .subscribe(value => {
              this.onFieldMappingChange(field.name_field, value);
            });
        this.syntheseForm.addControl(field.name_field, control);
      }
    }
  }

  // should activate the "rename mapping" button or gray it?
  renameMappingEnabled() {
    // a mapping have been selected and we have update right on it
    return this.fieldMappingForm.value != null && this.fieldMappingForm.value.cruved.U;
  }

  deleteMappingEnabled() {
    // a mapping have been selected and we have delete right on it
    return this.fieldMappingForm.value != null && this.fieldMappingForm.value.cruved.D;
  }

  showCreateMappingForm() {
    this.fieldMappingForm.reset();
    this.createOrRenameMappingForm.reset();
    this.createMappingFormVisible = true;
    this.renameMappingFormVisible = false;
    //this.displayAllValues = true; // XXX why?
  }

  showRenameMappingForm() {
    this.createOrRenameMappingForm.setValue(this.fieldMappingForm.value.mapping_label);
    this.createMappingFormVisible = false;
    this.renameMappingFormVisible = true;
  }

  hideCreateOrRenameMappingForm() {
    this.createMappingFormVisible = false;
    this.renameMappingFormVisible = false;
    this.createOrRenameMappingForm.reset();
  }

  createMapping() {
    this.spinner = true;
    this._ds.createMapping(this.createOrRenameMappingForm.value, "FIELD").pipe(
      finalize(() => {
        this.spinner = false;
        this.createMappingFormVisible = false;
      }),
    ).subscribe((mapping: Mapping) => {
        this.userFieldMappings.push(mapping);  // add the mapping to the list of known mapping (and so the select mapping form)
        this.fieldMappingForm.setValue(mapping); // automatically activate the created mapping
    }, (error: HttpErrorResponse) => {
      this._commonService.regularToaster('error', error.error.description);
    });
  }

  renameMapping(): void {
    this.spinner = true;
    this._ds.renameMapping(this.fieldMappingForm.value.id_mapping,
                       this.createOrRenameMappingForm.value).pipe(
      finalize(() => {
        this.spinner = false;
        this.renameMappingFormVisible = false;
      }),
    ).subscribe((mapping: Mapping) => {
      let index = this.userFieldMappings
                      .findIndex((m: Mapping) => m.id_mapping == mapping.id_mapping);
      this.fieldMappingForm.setValue(mapping);
      this.userFieldMappings[index] = mapping;
    }, (error: HttpErrorResponse) => {
      this._commonService.regularToaster('error', error.error.description);
    });
  }

  deleteMapping(): void {
    this.spinner = true;
    this._ds.deleteMapping(this.fieldMappingForm.value.id_mapping).pipe(
      finalize(() => this.spinner = false),
    ).subscribe((fieldMappings: Array<Mapping>) => {
      this.fieldMappingForm.setValue(null);
      this.userFieldMappings = fieldMappings;
    }, (error: HttpErrorResponse) => {
      console.log(error);
      this._commonService.regularToaster('error', error.error.description);
    });
  }

  onNewMappingSelected(mapping: Mapping): void {
    this.hideCreateOrRenameMappingForm();
    if (mapping == null) {
      this.syntheseForm.reset();
    } else {
      this.fillSyntheseFormWithMapping(mapping);
    }
  }

  /**
   * Fill the field form with the value define in the given mapping
   * @param mapping : id of the mapping
   */
  fillSyntheseFormWithMapping(mapping: Mapping) {
    // Retrieve fields for this mapping
    this._ds.getMappingFields(mapping.id_mapping).subscribe(
      mappingFields => {
        this.syntheseForm.reset();
        for (let field of mappingFields) {
          let control = this.syntheseForm.get(field.target_field);
          if (!control) continue;  // masked field?
          if (!this.sourceFields.includes(field.source_field)) continue;  // this field mapping does not apply to this file
          control.setValue(field.source_field);
        }
      },
      error => {
        this._commonService.regularToaster("error", error.error.description);
      }
    );
  }

  // a new source field have been selected for a given target field
  onFieldMappingChange(field: string, value: string) {
    if (value == null) {
      if (this.mappedTargetFields.has(field)) {
        this.mappedTargetFields.delete(field);
        this.unmappedTargetFields.add(field);
      }
    } else {
      if (this.unmappedTargetFields.has(field)) {
        this.unmappedTargetFields.delete(field);
        this.mappedTargetFields.add(field);
      }
    }

    this.mappedSourceFields.clear();
    this.unmappedSourceFields = new Set(this.sourceFields);
    for (let theme of this.targetFields) {
      for (let targetField of theme.fields) {
        let sourceField = this.syntheseForm.get(targetField.name_field).value;
        if (sourceField != null) {
          this.unmappedSourceFields.delete(sourceField);
          this.mappedSourceFields.add(sourceField);
        }
      }
    }
  }

  onPreviousStep() {
    this.importProcessService.navigateToPreviousStep(this.step);
  }

  isNextStepAvailable() {
    return this.syntheseForm.valid;
  }

  onNextStep() {
    if (!this.isNextStepAvailable()) { console.log("next step not available!"); return; }
    if (!this.syntheseForm.pristine) { // mapping model has been modified (or its a new model)
      this._modalService.open(this.saveMappingModal); // ask if we should save the mapping
    } else if (this.fieldMappingForm.value && this.fieldMappingForm.value.id_mapping != this.importData.id_field_mapping) { // a model has been selected (not-modified) and it must be affected to the import
      console.log("update mapping");
      this.submitMapping(true);  // note: as form is pristine, the update step will be skipped
    } else { // nothing to do
      console.log("navigate to next step", this.step);
      this.importProcessService.navigateToNextStep(this.step);
    }
  }

  saveAndUpdateMapping(permanent_mapping: boolean = false, mapping_name: string = ''): Observable<Mapping> {
    return of(1).pipe(
      concatMap(() => {
        if (permanent_mapping) { // create or re-use a permanent mapping
          if (mapping_name) { // create a mapping with the given name
            return this._ds.createMapping(mapping_name, 'field')
                           .pipe(map(mapping => [mapping, true]));
          } else { // reuse the currently selected mapping
            return of([this.fieldMappingForm.value, false]);
          }
        } else { // create a temporary mapping
          return this._ds.createMapping('', 'field')
                         .pipe(map(mapping => [mapping, true]));
        }
      }),
      concatMap(([mapping, created]: [Mapping, boolean]) => {
        // update the mapping if it has been created, or if it has been modified
        if (created || !this.syntheseForm.pristine) {
          return this._ds.updateMappingFields(mapping.id_mapping, this.syntheseForm.value)
                         .pipe(mapTo(mapping));
        } else {
          return of(mapping);
        }
      }),
    );
  }

  submitMapping(permanent_mapping: boolean = false, mapping_name: string = '') {
    this.spinner = true;
    this.saveAndUpdateMapping(permanent_mapping, mapping_name).pipe(
      concatMap((mapping: Mapping) => {
        if (this.importData.id_field_mapping != mapping.id_mapping) {
          return this._ds.setImportFieldMapping(this.importData.id_import, mapping.id_mapping);
        } else {
          return of(this.importData);
        }
      }),
      finalize(() => this.spinner = false),
    ).subscribe((importData: Import) => {
      this.importProcessService.setImportData(importData);
      this.importProcessService.navigateToLastStep();
    }, (error: HttpErrorResponse) => {
      this._commonService.regularToaster('error', error.error.description);
    });
  }
}
