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
import { startWith, pairwise, switchMap, concatMap, map, mapTo, skip, finalize, catchError } from "rxjs/operators";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";

import { CommonService } from "@geonature_common/service/common.service";
import { CruvedStoreService } from "@geonature_common/service/cruved-store.service";

import { DataService } from "../../../services/data.service";
import { FieldMappingService } from "../../../services/mappings/field-mapping.service";
import { ModuleConfig } from "../../../module.config";
import { Import, SynthesisThemeFields } from "../../../models/import.model";
import { ImportProcessComponent } from "../import-process.component";
import { ImportProcessService } from "../import-process.service";
import { FieldMapping, FieldMappingValues } from "../../../models/mapping.model";
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
  public userFieldMappings: Array<FieldMapping>; // all field mapping accessible by the users

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
  public updateAvailable: boolean = false;
  public mappingSelected: boolean = false;
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
    public cruvedStore: CruvedStoreService,
  ) { }

  ngOnInit() {
    this.step = this._route.snapshot.data.step;
    this.importData = this.importProcessService.getImportData();
    this.canCreateMapping = this.cruvedStore.cruved.IMPORT.module_objects.MAPPING.cruved.C > 0;
    this.canRenameMapping = this.cruvedStore.cruved.IMPORT.module_objects.MAPPING.cruved.U > 0;
    this.canDeleteMapping = this.cruvedStore.cruved.IMPORT.module_objects.MAPPING.cruved.D > 0;
    forkJoin({
      fieldMappings: this._ds.getFieldMappings(),
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
      if (this.importData.fieldmapping) {
        this.fillSyntheseFormWithMapping(this.importData.fieldmapping);
      }

      // subscribe to changes of selected field mapping
      this.fieldMappingForm.valueChanges.pipe(
        skip(1),  // skip first empty value to avoid reseting the synthese form
      ).subscribe(mapping => {
        this.onNewMappingSelected(mapping);
      });

      this.formReady = true;
    });
  }

  // Used by select component to compare field mappings
  areMappingFieldEqual(fm1: FieldMapping, fm2: FieldMapping): boolean {
    return fm1 != null && fm2 != null && fm1.id === fm2.id;
  }

  // add a form control for each target field in the syntheseForm
  // mandatory target fields have a required validator
  populateSyntheseForm() {
    let validators: Array<ValidatorFn>;
    for (let themefields of this.targetFields) {
      for (let field of themefields.fields) {
        if (!field.autogenerated) {  // autogenerated = checkbox
          this.unmappedTargetFields.add(field.name_field);
        }
        if (field.mandatory) {
          validators = [Validators.required];
        } else {
          validators = [];
        }
        let control = new FormControl(null, validators);
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
      this.spinner = true
      this._ds.createFieldMapping(this.modalCreateMappingForm.value, this.getFieldMappingValues()).pipe()
          .subscribe( () => {
              this.processNextStep()
          },
          () => {
            this.spinner = false
          }
      )
  }
  updateMapping() {
    this.spinner = true
    let name = ''
    if (this.modalCreateMappingForm.value != this.fieldMappingForm.value.label) {
        name = this.modalCreateMappingForm.value
    }
    this._ds.updateFieldMapping(this.fieldMappingForm.value.id, this.getFieldMappingValues(), name).pipe()
        .subscribe( () => {
            this.processNextStep()
        },
        () => {
            this.spinner = false
        }
    )
  }
  deleteMapping() {
      this.spinner = true
      let mapping_id = this.fieldMappingForm.value.id
      this._ds.deleteFieldMapping(mapping_id).pipe()
          .subscribe(() => {
              this._commonService.regularToaster(
                  "success",
                  "Le mapping " + this.fieldMappingForm.value.label + " a bien été supprimé"
              )
              this.fieldMappingForm.setValue(null)
              this.userFieldMappings = this.userFieldMappings.filter(mapping  => {return mapping.id !== mapping_id})
              this.spinner = false
          }, () => {
              this.spinner = false
          }
          )
  }

  renameMapping(): void {
    this.spinner = true;
    this._ds.renameFieldMapping(this.fieldMappingForm.value.id,
                       this.createOrRenameMappingForm.value).pipe(
      finalize(() => {
        this.spinner = false;
        this.spinner = false;
        this.renameMappingFormVisible = false;
      }),
    ).subscribe((mapping: FieldMapping) => {
      let index = this.userFieldMappings
                      .findIndex((m: FieldMapping) => m.id == mapping.id);
      this.fieldMappingForm.setValue(mapping);
      this.userFieldMappings[index] = mapping;
    });
  }

  onNewMappingSelected(mapping: FieldMapping): void {
    this.hideCreateOrRenameMappingForm();
    if (mapping == null) {
      this.syntheseForm.reset();
        this.mappingSelected = false
    } else {
      this.fillSyntheseFormWithMapping(mapping.values);
      this.mappingSelected = true
    }
  }

  /**
   * Fill the field form with the value define in the given mapping
   * @param mapping : id of the mapping
   */
  fillSyntheseFormWithMapping(mappingvalues: FieldMappingValues) {
    // Retrieve fields for this mapping
    this.syntheseForm.reset();
    for (const [target, source] of Object.entries(mappingvalues)) {
      let control = this.syntheseForm.get(target);
      if (!control) continue;  // masked field?
      if (!this.sourceFields.includes(source)) continue;  // this field mapping does not apply to this file
      control.setValue(source);
    }
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
    for (let themefields of this.targetFields) {
      for (let targetField of themefields.fields) {
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
    if (!this.isNextStepAvailable()) { return; }
    let mappingValue = this.fieldMappingForm.value
    if (this.syntheseForm.dirty && (this.canCreateMapping || mappingValue && mappingValue.cruved.U && !mappingValue.public)) {
        if (mappingValue && !mappingValue.public) {
            this.updateAvailable = true
            this.modalCreateMappingForm.setValue(mappingValue.label)
        }
        else {
            this.updateAvailable = false
            this.modalCreateMappingForm.setValue('')
        }
        this._modalService.open(this.saveMappingModal, { size: 'lg' })
    }
    else {
        this.spinner = true;
        this.processNextStep()
    }

  }
  processNextStep(){
      of(this.importData).pipe(
          concatMap((importData: Import) => {
              if (this.mappingSelected || this.syntheseForm.dirty) {
                  return this._ds.setImportFieldMapping(importData.id_import, this.getFieldMappingValues())
              } else {
                  return of(importData);
              }
          }),
          concatMap((importData: Import) => {
              if (!importData.source_count) {
                  return this._ds.loadImport(importData.id_import)
              } else {
                  return of(importData);
              }
          }),
          finalize(() => this.spinner = false),
      ).subscribe(
          (importData: Import) => {
              this.importProcessService.setImportData(importData);
              this.importProcessService.navigateToNextStep(this.step);
          }
      )
  }

  getFieldMappingValues(): FieldMappingValues {
      let values: FieldMappingValues = {};
      for (let [key, value] of Object.entries(this.syntheseForm.value)) {
        if (value != null) {
          values[key] = value as string;
        }
      }
      return values;
  }
}
